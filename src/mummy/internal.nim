import common, std/nativesockets, webby/httpheaders, std/endians, std/strutils,
    std/locks, std/atomics

template currentExceptionAsMummyError*(): untyped =
  let e = getCurrentException()
  newException(MummyError, e.getStackTrace & e.msg, e)

proc encodeFrameHeader*(
  opcode: uint8,
  payloadLen: int
): string {.raises: [], gcsafe.} =
  assert (opcode and 0b11110000) == 0

  # Calculate the frame header buffer len in advance to just do one allocation
  var frameHeaderLen = 2
  if payloadLen <= 125:
    discard
  elif payloadLen <= uint16.high.int:
    frameHeaderLen += 2
  else:
    frameHeaderLen += 8

  result = newStringOfCap(frameHeaderLen)
  result.add cast[char](0b10000000 or opcode)

  if payloadLen <= 125:
    result.add payloadLen.char
  elif payloadLen <= uint16.high.int:
    result.add 126.char
    var l = cast[uint16](payloadLen).htons
    result.setLen(result.len + 2)
    copyMem(result[result.len - 2].addr, l.addr, 2)
  else:
    result.add 127.char
    var l: uint64
    bigEndian64(l.addr, payloadLen.unsafeAddr)
    result.setLen(result.len + 8)
    copyMem(result[result.len - 8].addr, l.addr, 8)

proc encodeHeaders*(
  statusCode: int,
  headers: HttpHeaders
): string {.raises: [], gcsafe.} =
  let
    status =
      case statusCode:
      of 101:
        $statusCode & " Switching Protocols"
      else:
        $statusCode
    statusLineLen = 9 + status.len + 2

  # Calculate the header buffer len in advance to just do one allocation
  var headersLen = statusLineLen
  for (k, v) in headers:
    # k + ": " + v + "\r\n"
    headersLen += k.len + 2 + v.len + 2
  # "\r\n"
  headersLen += 2

  result = newString(headersLen)
  result[0] = 'H'
  result[1] = 'T'
  result[2] = 'T'
  result[3] = 'P'
  result[4] = '/'
  result[5] = '1'
  result[6] = '.'
  result[7] = '1'
  result[8] = ' '

  var pos = 9
  copyMem(
    result[pos].addr,
    status[0].unsafeAddr,
    status.len
  )
  pos += status.len

  result[pos + 0] = '\r'
  result[pos + 1] = '\n'
  pos += 2

  for (k, v) in headers:
    copyMem(
      result[pos].addr,
      k.cstring,
      k.len
    )
    pos += k.len

    result[pos + 0] = ':'
    result[pos + 1] = ' '
    pos += 2

    copyMem(
      result[pos].addr,
      v.cstring,
      v.len
    )
    pos += v.len

    result[pos + 0] = '\r'
    result[pos + 1] = '\n'
    pos += 2

  result[pos + 0] = '\r'
  result[pos + 1] = '\n'
  pos += 2

template integerOutOfRangeError() =
  raise newException(ValueError, "Parsed integer outside of valid range")

template invalidIntegerError() =
  raise newException(ValueError, "Invalid integer string")

template invalidHexError() =
  raise newException(ValueError, "Invalid hex string")

proc strictParseInt*(s: openarray[char]): int =
  var
    sign = -1
    i = 0

  if i < s.len and s[i] == '-':
    inc i
    sign = 1

  if i == s.len: # "-"
    invalidIntegerError()

  if i < s.len:
    if (i == 0 and s.len - i == 1 and s[i] == '0') or s[i] in {'1'..'9'}:
      result = 0
      while i < s.len and s[i] in {'0'..'9'}:
        let c = ord(s[i]) - ord('0')
        if result >= (int.low + c) div 10:
          result = result * 10 - c
        else:
          integerOutOfRangeError()
        inc i
      if sign == -1 and result == int.low:
        integerOutOfRangeError()
      else:
        result = result * sign

  if i == 0 or i != s.len:
    invalidIntegerError()

proc toHexWithoutLeadingZeroes*(i: int): string =
  if i == 0:
    return "0"
  result = toHex(i)
  for i, c in result:
    if c != '0':
      result = result[i .. ^1]
      break

proc strictParseHex*(s: openarray[char]): int =
  var
    i = 0
    bits: uint

  if s.len > 1 and s[i] == '0':
    invalidHexError()

  if s.len > 16:
    integerOutOfRangeError()

  while i < s.len:
    case s[i]
    of '0'..'9':
      bits = bits shl 4 or ord(s[i]).uint - ord('0').uint
    of 'a'..'f':
      bits = bits shl 4 or ord(s[i]).uint - ord('a').uint + 10.uint
    of 'A'..'F':
      bits = bits shl 4 or ord(s[i]).uint - ord('A').uint + 10.uint
    else:
      break
    inc i

  if i == 0 or i != s.len:
    invalidHexError()

  if bits > int.high.uint:
    integerOutOfRangeError()

  result = bits.int

const
  defaultStreamChannelCapacity* = 256 * 1024 # 256KB, must be power of 2

type
  StreamChannelObj* = object
    lock*: Lock
    cond*: Cond
    buf*: string
    head*, tail*: int          # head = write position, tail = read position
    capacity*: int             # buf.len, power of 2
    closed*: bool              # EOF signaled
    error*: bool               # Connection error
    pausedReading*: bool       # IO thread stopped reading from socket

  StreamChannel* = ptr StreamChannelObj

proc initStreamChannel*(capacity = defaultStreamChannelCapacity): StreamChannel =
  assert (capacity and (capacity - 1)) == 0, "capacity must be a power of 2"
  result = cast[StreamChannel](allocShared0(sizeof(StreamChannelObj)))
  initLock(result.lock)
  initCond(result.cond)
  result.buf.setLen(capacity)
  result.capacity = capacity

proc available*(channel: StreamChannel): int {.inline.} =
  ## Returns the number of bytes available to read. Caller must hold lock.
  (channel.head - channel.tail + channel.capacity) and (channel.capacity - 1)

proc freeSpace*(channel: StreamChannel): int {.inline.} =
  ## Returns the number of bytes that can be written. Caller must hold lock.
  channel.capacity - 1 - channel.available

proc writeChannel*(channel: StreamChannel, data: pointer, len: int): int =
  ## Writes up to len bytes into the channel from data.
  ## Called by the IO thread. Returns the number of bytes written.
  ## Caller must hold lock.
  if len <= 0 or channel.closed:
    return 0
  let toWrite = min(len, channel.freeSpace)
  if toWrite <= 0:
    return 0
  let mask = channel.capacity - 1
  let headIdx = channel.head and mask
  # How much fits before wrapping
  let firstPart = min(toWrite, channel.capacity - headIdx)
  copyMem(channel.buf[headIdx].addr, data, firstPart)
  if firstPart < toWrite:
    # Wrap around
    copyMem(
      channel.buf[0].addr,
      cast[pointer](cast[uint](data) + firstPart.uint),
      toWrite - firstPart
    )
  channel.head += toWrite
  result = toWrite

proc readChannel*(channel: StreamChannel, buf: var string, maxBytes: int): int =
  ## Reads up to maxBytes from the channel into buf.
  ## Called by the worker thread. Caller must hold lock.
  ## Returns the number of bytes read.
  if maxBytes <= 0:
    return 0
  let avail = channel.available
  if avail == 0:
    return 0
  let toRead = min(maxBytes, avail)
  let mask = channel.capacity - 1
  let tailIdx = channel.tail and mask
  let oldLen = buf.len
  buf.setLen(oldLen + toRead)
  let firstPart = min(toRead, channel.capacity - tailIdx)
  copyMem(buf[oldLen].addr, channel.buf[tailIdx].addr, firstPart)
  if firstPart < toRead:
    copyMem(buf[oldLen + firstPart].addr, channel.buf[0].addr, toRead - firstPart)
  channel.tail += toRead
  result = toRead

proc closeChannel*(channel: StreamChannel) =
  ## Marks the channel as closed (EOF). Caller must hold lock.
  channel.closed = true

proc errorChannel*(channel: StreamChannel) =
  ## Marks the channel as errored and closed. Caller must hold lock.
  channel.error = true
  channel.closed = true

proc destroyStreamChannel*(channel: StreamChannel) =
  ## Frees the stream channel. Must not be in use by any thread.
  if channel != nil:
    deinitLock(channel.lock)
    deinitCond(channel.cond)
    `=destroy`(channel[])
    deallocShared(channel)
