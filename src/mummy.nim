when not defined(nimdoc):
  when not defined(gcArc) and not defined(gcOrc) and not defined(gcAtomicArc):
    {.error: "Using --mm:arc, --mm:orc or --mm:atomicArc is required by Mummy.".}

when not compileOption("threads"):
  {.error: "Using --threads:on is required by Mummy.".}

import mummy/common, mummy/internal, std/atomics, std/base64,
    std/cpuinfo, std/deques, std/hashes, std/nativesockets, std/os,
    std/parseutils, std/random, std/selectors, std/sets, crunchy,
    std/tables, std/times, webby/httpheaders, webby/queryparams, webby/urls,
    zippy, std/options

from std/strutils import find, cmpIgnoreCase, toLowerAscii

when defined(linux):
  when defined(nimdoc):
    # Why am I doing this?
    from std/posix import write, TPollfd, POLLIN, poll, close, EAGAIN, O_CLOEXEC, O_NONBLOCK
  else:
    import std/posix

  let SOCK_NONBLOCK
    {.importc: "SOCK_NONBLOCK", header: "<sys/socket.h>".}: cint

import std/locks

export Port, common, httpheaders, queryparams

const
  whitespace = {' ', '\t'}
  listenBacklogLen = 128
  maxEventsPerSelectLoop = 64
  initialRecvBufLen = (4 * 1024) - 9 # 8 byte cap field + null terminator

let
  http10 = "HTTP/1.0"
  http11 = "HTTP/1.1"

type
  ResponseStreamObj = object
    server: Server
    clientSocket: SocketHandle
    clientId: uint64
    closeConnection: bool
    httpVersion: HttpVersion
    http10Mode: bool           # HTTP/1.0: no chunk framing, Connection: close
    lock: Lock
    cond: Cond
    pendingBytes: Atomic[int]
    maxPendingBytes: int
    finished: bool
    error: Atomic[bool]

  ResponseStream* = ptr ResponseStreamObj
    ## A handle for streaming response data in chunks.

  RequestStreamObj = object
    server: Server
    clientSocket: SocketHandle
    clientId: uint64
    channel: StreamChannel
    contentLength: int         # -1 if chunked (unknown total)
    bytesRead: int

  RequestStream* = ptr RequestStreamObj
    ## A handle for reading streaming request body data.

  RequestObj* = object
    httpVersion*: HttpVersion ## HTTP version from the request line.
    httpMethod*: string ## HTTP method from the request line.
    uri*: string ## Raw URI from the HTTP request line.
    path*: string ## Decoded request URI path.
    queryParams*: QueryParams ## Decoded request query parameter key-value pairs.
    pathParams*: PathParams ## Router named path parameter key-value pairs.
    headers*: HttpHeaders ## HTTP headers key-value pairs.
    body*: string ## Request body.
    remoteAddress*: string ## Network address of the request sender.
    server: Server
    clientSocket: SocketHandle
    clientId: uint64
    responded: bool
    requestStream*: RequestStream ## Non-nil when body is being streamed.
    responseStream*: ResponseStream ## Set by startResponse, used for cleanup.

  Request* = ptr RequestObj

  WebSocket* = object
    server: Server
    clientSocket: SocketHandle
    clientId: uint64

  Message* = object
    kind*: MessageKind
    data*: string

  WebSocketEvent* = enum
    OpenEvent, MessageEvent, ErrorEvent, CloseEvent

  MessageKind* = enum
    TextMessage, BinaryMessage, Ping, Pong

  RequestHandler* = proc(request: Request) {.gcsafe.}

  WebSocketHandler* = proc(
    websocket: WebSocket,
    event: WebSocketEvent,
    message: Message
  ) {.gcsafe.}

  ServerObj = object
    handler: RequestHandler
    websocketHandler: WebSocketHandler
    logHandler: LogHandler
    maxHeadersLen, maxBodyLen, maxMessageLen: int
    rand: Rand
    workerThreads: seq[Thread[Server]]
    serving: Atomic[bool]
    destroyCalled: bool
    socket: SocketHandle
    selector: Selector[DataEntry]
    responseQueued, sendQueued, shutdown: SelectEvent
    clientSockets: HashSet[SocketHandle]
    taskQueueLock: Lock
    taskQueueCond: Cond
    taskQueue: Deque[WorkerTask]
    responseQueue: Deque[OutgoingBuffer]
    responseQueueLock: Lock
    sendQueue: Deque[OutgoingBuffer]
    sendQueueLock: Lock
    websocketClaimed: Table[WebSocket, bool]
    websocketQueues: Table[WebSocket, Deque[WebSocketUpdate]]
    websocketQueuesLock: Lock
    streamingThreshold: int
    streamResumeReading: SelectEvent
    streamResumeQueue: Deque[SocketHandle]
    streamResumeQueueLock: Lock

  Server* = ptr ServerObj

  WorkerTask = object
    request: Request
    websocket: WebSocket

  DataEntryKind = enum
    ServerSocketEntry, ClientSocketEntry, EventEntry

  DataEntry {.acyclic.} = ref object
    case kind: DataEntryKind:
    of ServerSocketEntry:
      discard
    of EventEntry:
      event: SelectEvent
    of ClientSocketEntry:
      clientId: uint64
      remoteAddress: string
      recvBuf: string
      bytesReceived: int
      requestState: IncomingRequestState
      frameState: IncomingFrameState
      outgoingBuffers: Deque[OutgoingBuffer]
      closeFrameQueuedAt: float64
      upgradedToWebSocket, closeFrameSent: bool
      sendsWaitingForUpgrade: seq[OutgoingBuffer]
      requestCounter: int # Incoming request incs, outgoing response decs
      streamChannel: StreamChannel    # Non-nil when streaming upload is active
      streamingRequest: bool          # True while streaming body is being received
      streamChunked: bool             # True if streaming request uses chunked encoding
      streamBytesRemaining: int       # For Content-Length streaming: bytes left

  IncomingRequestState = object
    headersParsed: bool
    chunked: bool
    loggedUnexpectedData: bool
    contentLength: int
    httpVersion: HttpVersion
    httpMethod: string
    uri: string
    path: string
    queryParams: QueryParams
    headers: HttpHeaders
    body: string

  IncomingFrameState = object
    opcode: uint8
    buffer: string
    frameLen: int

  OutgoingBuffer {.acyclic.} = ref object
    clientSocket: SocketHandle
    clientId: uint64
    closeConnection, isWebSocketUpgrade, isCloseFrame: bool
    buffer1, buffer2: string
    bytesSent: int
    responseStream: ResponseStream # Non-nil for streaming response buffers

  WebSocketUpdate = object
    event: WebSocketEvent
    message: Message

proc `$`*(request: Request): string {.gcsafe.} =
  result = request.httpMethod & " " & request.uri & " "
  {.gcsafe.}:
    case request.httpVersion:
    of Http10:
      result &= http10
    else:
      result &= http11
  result &= " (" & $cast[uint](request) & ")"

proc `$`*(websocket: WebSocket): string =
  "WebSocket " & $cast[uint](hash(websocket))

proc log(server: Server, level: LogLevel, args: varargs[string]) =
  if server.logHandler == nil:
    return
  try:
    server.logHandler(level, args)
  except Exception as e:
    discard # ???

proc headerContainsToken(headers: var HttpHeaders, key, token: string): bool =
  # If a header looks like `Accept-Encoding: gzip,deflate` then we may want to
  # check if the value contains a specific token (in this case gzip or deflate)
  # This proc does a case-insensitive check while avoiding allocations
  for (k, v) in headers:
    if cmpIgnoreCase(k, key) == 0:
      var first = 0
      while first < v.len:
        var comma = v.find(',', start = first)
        if comma == -1:
          comma = v.len
        var len = comma - first
        while len > 0 and v[first] in whitespace:
          inc first
          dec len
        while len > 0 and v[first + len - 1] in whitespace:
          dec len
        if len > 0 and len == token.len:
          var matches = true
          for i in 0 ..< len:
            if ord(toLowerAscii(v[first + i])) != ord(toLowerAscii(token[i])):
              matches = false
              break
          if matches:
            return true
        first = comma + 1

proc registerHandle2(
  selector: Selector[DataEntry],
  socket: SocketHandle,
  events: set[Event],
  data: DataEntry
) {.raises: [IOSelectorsException].} =
  try:
    selector.registerHandle(socket, events, data)
  except ValueError: # Why ValueError?
    raise newException(IOSelectorsException, getCurrentExceptionMsg())

proc updateHandle2(
  selector: Selector[DataEntry],
  socket: SocketHandle,
  events: set[Event]
) {.raises: [IOSelectorsException].} =
  try:
    selector.updateHandle(socket, events)
  except ValueError: # Why ValueError?
    raise newException(IOSelectorsException, getCurrentExceptionMsg())

proc trigger(
  server: Server,
  event: SelectEvent
) {.raises: [].} =
  try:
    event.trigger()
  except Exception as e:
    let err = osLastError()
    server.log(
      ErrorLevel,
      "Error triggering event ", $err, " ", osErrorMsg(err)
    )

proc send*(
  websocket: WebSocket,
  data: sink string,
  kind = TextMessage,
) {.raises: [], gcsafe.} =
  ## Enqueues the message to be sent over the WebSocket connection.

  var encodedFrame = OutgoingBuffer()
  encodedFrame.clientSocket = websocket.clientSocket
  encodedFrame.clientId = websocket.clientId

  case kind:
  of TextMessage:
    encodedFrame.buffer1 = encodeFrameHeader(0x1, data.len)
  of BinaryMessage:
    encodedFrame.buffer1 = encodeFrameHeader(0x2, data.len)
  of Ping:
    encodedFrame.buffer1 = encodeFrameHeader(0x9, data.len)
  of Pong:
    encodedFrame.buffer1 = encodeFrameHeader(0xA, data.len)

  encodedFrame.buffer2 = move data

  var queueWasEmpty: bool
  withLock websocket.server.sendQueueLock:
    queueWasEmpty = websocket.server.sendQueue.len == 0
    websocket.server.sendQueue.addLast(move encodedFrame)

  if queueWasEmpty:
    websocket.server.trigger(websocket.server.sendQueued)

proc close*(websocket: WebSocket) {.raises: [], gcsafe.} =
  ## Begins the WebSocket closing handshake.
  ## This does not discard previously queued messages before starting the
  ## closing handshake.
  ## The handshake will only begin after the queued messages are sent.

  var encodedFrame = OutgoingBuffer()
  encodedFrame.clientSocket = websocket.clientSocket
  encodedFrame.clientId = websocket.clientId
  encodedFrame.buffer1 = encodeFrameHeader(0x8, 0)
  encodedFrame.isCloseFrame = true

  var queueWasEmpty: bool
  withLock websocket.server.sendQueueLock:
    queueWasEmpty = websocket.server.sendQueue.len == 0
    websocket.server.sendQueue.addLast(move encodedFrame)

  if queueWasEmpty:
    websocket.server.trigger(websocket.server.sendQueued)

proc respond*(
  request: Request,
  statusCode: int,
  headers: sink HttpHeaders = emptyHttpHeaders(),
  body: sink string = ""
) {.raises: [], gcsafe.} =
  ## Sends the response for the request.
  ## This should usually only be called once per request.

  if request.responded:
    request.server.log(
      InfoLevel,
      "Responding to a request that has already received a non-1xx response"
    )

  var encodedResponse = OutgoingBuffer()
  encodedResponse.clientSocket = request.clientSocket
  encodedResponse.clientId = request.clientId
  encodedResponse.closeConnection =
    request.httpVersion == Http10 # Default behavior

  # Override default behavior based on request Connection header
  if request.headers.headerContainsToken("Connection", "close"):
    encodedResponse.closeConnection = true
  elif request.headers.headerContainsToken("Connection", "keep-alive"):
    encodedResponse.closeConnection = false

  # If we are not already going to close the connection based on the request
  # headers, check if we should based on the response headers
  if not encodedResponse.closeConnection:
    encodedResponse.closeConnection = headers.headerContainsToken(
      "Connection", "close"
    )

  if encodedResponse.closeConnection:
    headers["Connection"] = "close"
  elif request.httpVersion == Http10:
    headers["Connection"] = "keep-alive"

  # If the body is big enough to justify compressing and not already compressed
  if body.len > 860 and "Content-Encoding" notin headers:
    if request.headers.headerContainsToken("Accept-Encoding", "gzip"):
      try:
        body = compress(body.cstring, body.len, BestSpeed, dfGzip)
        headers["Content-Encoding"] = "gzip"
      except Exception as e:
        # This should never happen since exceptions are only thrown if
        # the data format is invalid or the level is invalid
        request.server.log(
          DebugLevel,
          "Unexpected gzip error: " & e.msg
        )
    elif request.headers.headerContainsToken("Accept-Encoding", "deflate"):
      try:
        body = compress(body.cstring, body.len, BestSpeed, dfDeflate)
        headers["Content-Encoding"] = "deflate"
      except Exception as e:
        # See gzip
        request.server.log(
          DebugLevel,
          "Unexpected deflate error: " & e.msg
        )
    else:
      discard

  # This is usually not set by the caller, however it needs to be for HEAD
  # responses where there is a Content-Length but no body
  if "Content-Length" notin headers:
    let shouldAddContentLengthHeader =
      statusCode != 204 and (statusCode < 100 or statusCode >= 200)
    # Do not add a Content-Length header for a 204 or 1xx response
    # See RFC 7230 3.3.2
    if shouldAddContentLengthHeader or body.len > 0:
      headers["Content-Length"] = $body.len

  encodedResponse.buffer1 = encodeHeaders(statusCode, headers)
  if encodedResponse.buffer1.len + body.len < 32 * 1024:
    # There seems to be a harsh penalty on multiple send() calls on Linux
    # so just use 1 buffer if the body is small enough
    encodedResponse.buffer1 &= body
  else:
    encodedResponse.buffer2 = move body
  encodedResponse.isWebSocketUpgrade = headers.headerContainsToken(
    "Upgrade",
    "websocket"
  )

  if statusCode < 100 or statusCode >= 200:
    # Mark if this request has received a non-informational (1xx) response
    request.responded = true

  var queueWasEmpty: bool
  withLock request.server.responseQueueLock:
    queueWasEmpty = request.server.responseQueue.len == 0
    request.server.responseQueue.addLast(move encodedResponse)

  if queueWasEmpty:
    request.server.trigger(request.server.responseQueued)

proc startResponse*(
  request: Request,
  statusCode: int,
  headers: sink HttpHeaders = emptyHttpHeaders(),
  maxPendingBytes = 256 * 1024
): ResponseStream {.raises: [], gcsafe.} =
  ## Begins a streaming response with chunked transfer encoding.
  ## Returns a ResponseStream that the handler uses to send body chunks.
  ## The handler MUST call finish() on the returned stream.

  let stream = cast[ResponseStream](allocShared0(sizeof(ResponseStreamObj)))
  stream.server = request.server
  stream.clientSocket = request.clientSocket
  stream.clientId = request.clientId
  stream.httpVersion = request.httpVersion
  stream.maxPendingBytes = maxPendingBytes
  initLock(stream.lock)
  initCond(stream.cond)

  stream.closeConnection =
    request.httpVersion == Http10

  if request.headers.headerContainsToken("Connection", "close"):
    stream.closeConnection = true
  elif request.headers.headerContainsToken("Connection", "keep-alive"):
    stream.closeConnection = false

  if not stream.closeConnection:
    stream.closeConnection = headers.headerContainsToken("Connection", "close")

  if request.httpVersion == Http10:
    # HTTP/1.0 does not support chunked transfer encoding
    # Omit Content-Length, use Connection: close
    stream.http10Mode = true
    stream.closeConnection = true
    headers["Connection"] = "close"
  else:
    headers["Transfer-Encoding"] = "chunked"
    if stream.closeConnection:
      headers["Connection"] = "close"

  var encodedResponse = OutgoingBuffer()
  encodedResponse.clientSocket = request.clientSocket
  encodedResponse.clientId = request.clientId
  encodedResponse.buffer1 = encodeHeaders(statusCode, headers)
  # Don't set closeConnection on the header buffer since more chunks follow

  request.responded = true
  request.responseStream = stream

  var queueWasEmpty: bool
  withLock request.server.responseQueueLock:
    queueWasEmpty = request.server.responseQueue.len == 0
    request.server.responseQueue.addLast(move encodedResponse)

  if queueWasEmpty:
    request.server.trigger(request.server.responseQueued)

  result = stream

proc write*(
  stream: ResponseStream,
  data: sink string
) {.raises: [], gcsafe.} =
  ## Sends a chunk of body data over the streaming response.
  ## Blocks if the backpressure threshold is exceeded.
  ## For HTTP/1.1, data is chunk-encoded per RFC 7230 Section 4.1.

  if data.len == 0:
    return # Empty write is a no-op (0-length chunk = end of body)

  if stream.error.load(moRelaxed):
    return

  var encodedChunk = OutgoingBuffer()
  encodedChunk.clientSocket = stream.clientSocket
  encodedChunk.clientId = stream.clientId
  encodedChunk.responseStream = stream

  if stream.http10Mode:
    encodedChunk.buffer1 = move data
  else:
    # Chunk encoding: hexLen\r\n + data + \r\n
    let hexLen = toHexWithoutLeadingZeroes(data.len)
    encodedChunk.buffer1 = newStringOfCap(hexLen.len + 2 + data.len + 2)
    encodedChunk.buffer1.add(hexLen)
    encodedChunk.buffer1.add("\r\n")
    encodedChunk.buffer1.add(data)
    encodedChunk.buffer1.add("\r\n")

  let bufferBytes = encodedChunk.buffer1.len + encodedChunk.buffer2.len
  discard stream.pendingBytes.fetchAdd(bufferBytes)

  var queueWasEmpty: bool
  withLock stream.server.responseQueueLock:
    queueWasEmpty = stream.server.responseQueue.len == 0
    stream.server.responseQueue.addLast(move encodedChunk)

  if queueWasEmpty:
    stream.server.trigger(stream.server.responseQueued)

  # Backpressure: block if too many bytes pending
  if stream.pendingBytes.load(moRelaxed) >= stream.maxPendingBytes:
    withLock stream.lock:
      while stream.pendingBytes.load(moRelaxed) >= stream.maxPendingBytes and
          not stream.error.load(moRelaxed):
        wait(stream.cond, stream.lock)

proc finish*(stream: ResponseStream) {.raises: [], gcsafe.} =
  ## Sends the terminal chunk and cleans up the stream.
  ## After this call the ResponseStream must not be used.

  if not stream.finished:
    stream.finished = true

    if not stream.error.load(moRelaxed):
      var terminalChunk = OutgoingBuffer()
      terminalChunk.clientSocket = stream.clientSocket
      terminalChunk.clientId = stream.clientId
      terminalChunk.closeConnection = stream.closeConnection

      if stream.http10Mode:
        # HTTP/1.0: just close the connection (no terminal chunk)
        terminalChunk.buffer1 = ""
      else:
        terminalChunk.buffer1 = "0\r\n\r\n"

      var queueWasEmpty: bool
      withLock stream.server.responseQueueLock:
        queueWasEmpty = stream.server.responseQueue.len == 0
        stream.server.responseQueue.addLast(move terminalChunk)

      if queueWasEmpty:
        stream.server.trigger(stream.server.responseQueued)

proc destroyResponseStream(stream: ResponseStream) =
  if stream != nil:
    deinitLock(stream.lock)
    deinitCond(stream.cond)
    `=destroy`(stream[])
    deallocShared(stream)

proc hasError*(stream: ResponseStream): bool {.raises: [], gcsafe.} =
  ## Returns true if the connection was closed/errored during streaming.
  stream.error.load(moRelaxed)

proc isStreaming*(request: Request): bool {.inline.} =
  ## Returns true if this request has a streaming body.
  request.requestStream != nil

proc read*(request: Request, maxBytes: int = 65536): string {.gcsafe.} =
  ## Reads up to maxBytes from the request body stream.
  ## Returns the data read. Returns "" when the body is complete (EOF).
  ## Blocks if no data is available yet.

  if request.requestStream == nil:
    return ""

  let
    rs = request.requestStream
    channel = rs.channel

  withLock channel.lock:
    # Wait until data is available or channel is closed
    while channel.available == 0 and not channel.closed:
      wait(channel.cond, channel.lock)

    if channel.available == 0 and channel.closed:
      return "" # EOF

    let bytesRead = channel.readChannel(result, maxBytes)
    rs.bytesRead += bytesRead

    # If we were paused and now have free space, signal resume
    if channel.pausedReading and channel.freeSpace > channel.capacity div 4:
      channel.pausedReading = false
      var queueWasEmpty: bool
      withLock rs.server.streamResumeQueueLock:
        queueWasEmpty = rs.server.streamResumeQueue.len == 0
        rs.server.streamResumeQueue.addLast(rs.clientSocket)
      if queueWasEmpty:
        rs.server.trigger(rs.server.streamResumeReading)

proc readAll*(request: Request): string {.gcsafe.} =
  ## Reads the entire remaining request body stream.
  ## This buffers everything in memory.

  if request.requestStream == nil:
    return request.body

  while true:
    let chunk = request.read()
    if chunk.len == 0:
      break
    result.add(chunk)

proc destroyRequestStream(rs: RequestStream) =
  if rs != nil:
    destroyStreamChannel(rs.channel)
    `=destroy`(rs[])
    deallocShared(rs)

proc upgradeToWebSocket*(
  request: Request
): WebSocket {.raises: [MummyError], gcsafe.} =
  ## Upgrades the request to a WebSocket connection. You can immediately start
  ## calling send().
  ## Future updates for this WebSocket will be calls to the websocketHandler
  ## provided to `newServer`. The first event will be onOpen.
  ## Note: if the client disconnects before receiving this upgrade response,
  ## no onOpen event will be received.
  if not request.headers.headerContainsToken("Connection", "Upgrade"):
    raise newException(
      MummyError,
      "Invalid request to upgade, missing 'Connection: upgrade' header"
    )

  if not request.headers.headerContainsToken("Upgrade", "websocket"):
    raise newException(
      MummyError,
      "Invalid request to upgade, missing 'Upgrade: websocket' header"
    )

  let websocketKey = request.headers["Sec-WebSocket-Key"]
  if websocketKey == "":
    raise newException(
      MummyError,
      "Invalid request to upgade, missing Sec-WebSocket-Key header"
    )

  let websocketVersion = request.headers["Sec-WebSocket-Version"]
  if websocketVersion != "13":
    raise newException(
      MummyError,
      "Invalid request to upgade, missing Sec-WebSocket-Version header"
    )

  # Looks good to upgrade

  result = WebSocket(
    server: request.server,
    clientSocket: request.clientSocket,
    clientId: request.clientId
  )

  let hash = sha1(websocketKey & "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")

  var headers: HttpHeaders
  headers["Connection"] = "Upgrade"
  headers["Upgrade"] = "websocket"
  headers["Sec-WebSocket-Accept"] = base64.encode(hash)

  request.respond(101, headers)

proc workerProc(server: Server) {.raises: [].} =
  # The worker threads run the task queue here
  let server = server

  proc runTask(task: WorkerTask) =
    if task.request != nil:
      try:
        server.handler(task.request)
      except Exception as e:
        server.log(
          ErrorLevel,
          "Handler exception: " & e.msg & " " & e.getStackTrace()
        )
        if task.request.responseStream != nil:
          # If a streaming response was started, finish it (closes connection)
          if not task.request.responseStream.finished:
            task.request.responseStream.closeConnection = true
            task.request.responseStream.finish()
        elif not task.request.responded:
          task.request.respond(500)
      # Clean up streaming resources
      if task.request.responseStream != nil:
        if not task.request.responseStream.finished:
          task.request.responseStream.finish()
        destroyResponseStream(task.request.responseStream)
        task.request.responseStream = nil
      if task.request.requestStream != nil:
        destroyRequestStream(task.request.requestStream)
        task.request.requestStream = nil
      `=destroy`(task.request[])
      deallocShared(task.request)
    else: # WebSocket
      withLock server.websocketQueuesLock:
        if server.websocketClaimed.getOrDefault(task.websocket, true):
          # If this websocket has been claimed or if it is not present in
          # the table (which indicates it has been closed), skip this task
          return
        # Claim this websocket
        server.websocketClaimed[task.websocket] = true

      while true: # Process the entire websocket queue
        var update: Option[WebSocketUpdate]
        withLock server.websocketQueuesLock:
          try:
            if server.websocketQueues[task.websocket].len > 0:
              update = some(server.websocketQueues[task.websocket].popFirst())
              if update.get.event == CloseEvent:
                server.websocketQueues.del(task.websocket)
                server.websocketClaimed.del(task.websocket)
            else:
              server.websocketClaimed[task.websocket] = false
          except KeyError:
            discard # Not possible

        if not update.isSome:
          break

        try:
          server.websocketHandler(
            task.websocket,
            update.get.event,
            move update.get.message
          )
        except Exception as e:
          server.log(
            ErrorLevel,
            "WebSocket exception: " & e.msg & " " & e.getStackTrace()
          )

        if update.get.event == CloseEvent:
          break

  when defined(mummyCheck22398):
    var loggedExceptionLeak: bool

  while true:
    acquire(server.taskQueueLock)

    while server.taskQueue.len == 0 and not server.destroyCalled:
      wait(server.taskQueueCond, server.taskQueueLock)

    if server.destroyCalled:
      release(server.taskQueueLock)
      return

    let task = server.taskQueue.popFirst()
    release(server.taskQueueLock)

    runTask(task)

    when defined(mummyCheck22398):
      # https://github.com/nim-lang/Nim/issues/22398
      if not loggedExceptionLeak and getCurrentExceptionMsg() != "":
        echo "Detected leaked exception: ", getCurrentExceptionMsg()
        loggedExceptionLeak = true

proc postTask(server: Server, task: WorkerTask) {.raises: [].} =
  withLock server.taskQueueLock:
    server.taskQueue.addLast(task)
  signal(server.taskQueueCond)

proc postWebSocketUpdate(
  websocket: WebSocket,
  update: sink WebSocketUpdate
) {.raises: [].} =
  if websocket.server.websocketHandler == nil:
    websocket.server.log(DebugLevel, "WebSocket event but no WebSocket handler")
    return

  var needsTask: bool

  withLock websocket.server.websocketQueuesLock:
    if websocket notin websocket.server.websocketQueues:
      return

    try:
      websocket.server.websocketQueues[websocket].addLast(move update)
      if not websocket.server.websocketClaimed[websocket]:
        needsTask = true
    except KeyError:
      discard # Not possible

  if needsTask:
    websocket.server.postTask(WorkerTask(websocket: websocket))

proc sendCloseFrame(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry,
  closeConnection: bool
) {.raises: [IOSelectorsException].} =
  let outgoingBuffer = OutgoingBuffer()
  outgoingBuffer.clientSocket = clientSocket
  outgoingBuffer.clientId = dataEntry.clientId
  outgoingBuffer.buffer1 = encodeFrameHeader(0x8, 0)
  outgoingBuffer.isCloseFrame = true
  outgoingBuffer.closeConnection = closeConnection
  dataEntry.outgoingBuffers.addLast(outgoingBuffer)
  dataEntry.closeFrameQueuedAt = epochTime()
  server.selector.updateHandle2(clientSocket, {Read, Write})

proc afterRecvWebSocket(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  if dataEntry.closeFrameQueuedAt > 0 and
    epochTime() - dataEntry.closeFrameQueuedAt > 10:
    # The Close frame dance didn't work out, just close the connection
    return true

  # Try to parse entire frames out of the receive buffer
  while true:
    if dataEntry.bytesReceived < 2:
      return false # Need to receive more bytes

    let
      b0 = dataEntry.recvBuf[0].uint8
      b1 = dataEntry.recvBuf[1].uint8
      fin = (b0 and 0b10000000) != 0
      rsv1 = b0 and 0b01000000
      rsv2 = b0 and 0b00100000
      rsv3 = b0 and 0b00010000
      opcode = b0 and 0b00001111

    if rsv1 != 0 or rsv2 != 0 or rsv3 != 0:
      return true # Per spec this must fail, close the connection

    # Masking bit should be set
    if (b1 and 0b10000000) == 0:
      return true # Per spec, close the connection

    if opcode == 0 and dataEntry.frameState.opcode == 0:
      # Per spec, the first frame must have an opcode > 0
      return true # Close the connection

    if dataEntry.frameState.opcode != 0 and opcode != 0:
      # Per spec, if we have buffered fragments the opcode must be 0
      return true # Close the connection

    var pos = 2

    var payloadLen = (b1 and 0b01111111).int
    if payloadLen <= 125:
      discard
    elif payloadLen == 126:
      if dataEntry.bytesReceived < 4:
        return false # Need to receive more bytes
      var l: uint16
      copyMem(l.addr, dataEntry.recvBuf[pos].addr, 2)
      payloadLen = nativesockets.htons(l).int
      pos += 2
    else:
      if dataEntry.bytesReceived < 10:
        return false # Need to receive more bytes
      var l: uint32
      copyMem(l.addr, dataEntry.recvBuf[pos + 4].addr, 4)
      payloadLen = nativesockets.htonl(l).int
      pos += 8

    let isControlFrame = opcode in [0x8.uint8, 0x9, 0xA]
    if isControlFrame and not fin:
      # Per spec, control frames must not be fragmented
      return true # Close the connection
    if payloadLen > 125 and isControlFrame:
      # Per spec, control frames are only allowed payloads up to 125 bytes
      return true # Close the connection

    if dataEntry.frameState.frameLen + payloadLen > server.maxMessageLen:
      server.log(DebugLevel, "Dropped WebSocket, message too long")
      return true # Message is too large, close the connection

    if dataEntry.bytesReceived < pos + 4:
      return false # Need to receive more bytes

    var mask: array[4, uint8]
    copyMem(mask.addr, dataEntry.recvBuf[pos].addr, 4)

    pos += 4

    if dataEntry.bytesReceived < pos + payloadLen:
      return false # Need to receive more bytes

    # Unmask the payload
    for i in 0 ..< payloadLen:
      let j = i mod 4
      dataEntry.recvBuf[pos + i] =
        (dataEntry.recvBuf[pos + i].uint8 xor mask[j]).char

    if dataEntry.frameState.opcode == 0:
      # This is the first fragment
      dataEntry.frameState.opcode = opcode

    # Make room in the message buffer for this fragment
    let newFrameLen = dataEntry.frameState.frameLen + payloadLen
    if dataEntry.frameState.buffer.len < newFrameLen:
      let newBufferLen = max(dataEntry.frameState.buffer.len * 2, newFrameLen)
      dataEntry.frameState.buffer.setLen(newBufferLen)

    if payloadLen > 0:
      # Copy the fragment into the message buffer
      copyMem(
        dataEntry.frameState.buffer[dataEntry.frameState.frameLen].addr,
        dataEntry.recvBuf[pos].addr,
        payloadLen
      )
      dataEntry.frameState.frameLen += payloadLen

    # Remove this frame from the receive buffer
    let frameLen = pos + payloadLen
    if dataEntry.bytesReceived == frameLen:
      dataEntry.bytesReceived = 0
    else:
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[frameLen].addr,
        dataEntry.bytesReceived - frameLen
      )
      dataEntry.bytesReceived -= frameLen

    if fin:
      let frameOpcode = dataEntry.frameState.opcode

      # We have a full message

      var message: Message
      message.data = move dataEntry.frameState.buffer
      message.data.setLen(dataEntry.frameState.frameLen)

      dataEntry.frameState = IncomingFrameState()

      case frameOpcode:
      of 0x1: # Text
        message.kind = TextMessage
      of 0x2: # Binary
        message.kind = BinaryMessage
      of 0x8: # Close
        # If we already queued a close, just close the connection
        # This is not quite perfect
        if dataEntry.closeFrameQueuedAt > 0:
          return true # Close the connection
        # Otherwise send a Close in response then close the connection
        server.sendCloseFrame(clientSocket, dataEntry, true)
        continue
      of 0x9: # Ping
        message.kind = Ping
      of 0xA: # Pong
        message.kind = Pong
      else:
        server.log(DebugLevel, "Dropped WebSocket, received invalid opcode")
        return true # Invalid opcode, close the connection

      let
        websocket = WebSocket(
          server: server,
          clientSocket: clientSocket,
          clientId: dataEntry.clientId
        )
        update = WebSocketUpdate(
          event: MessageEvent,
          message: move message
        )
      websocket.postWebSocketUpdate(update)

proc popRequest(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): Request {.raises: [].} =
  ## Pops the completed HttpRequest from the socket and resets the parse state.
  result = cast[Request](allocShared0(sizeof(RequestObj)))
  result.server = server
  result.clientSocket = clientSocket
  result.clientId = dataEntry.clientId
  result.remoteAddress = dataEntry.remoteAddress
  result.httpVersion = dataEntry.requestState.httpVersion
  result.httpMethod = move dataEntry.requestState.httpMethod
  result.uri = move dataEntry.requestState.uri
  result.path = move dataEntry.requestState.path
  result.queryParams = move dataEntry.requestState.queryParams
  result.headers = move dataEntry.requestState.headers
  result.body = move dataEntry.requestState.body
  if not dataEntry.streamingRequest:
    result.body.setLen(dataEntry.requestState.contentLength)
  dataEntry.requestState = IncomingRequestState()
  inc dataEntry.requestCounter
  if dataEntry.bytesReceived > 0 and not dataEntry.streamingRequest:
    server.log(DebugLevel, "Receive buffer not empty after request")

proc afterRecvHttp(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  # We do not expect pipelined requests so log if any new data is received
  # while a request is outstanding (except when streaming body data)
  if dataEntry.requestCounter > 0 and
    not dataEntry.requestState.loggedUnexpectedData and
    not dataEntry.streamingRequest:
    server.log(
      DebugLevel,
      "Received data before the previous request has been responded to"
    )
    dataEntry.requestState.loggedUnexpectedData = true

  # If we're already streaming body data, skip header parsing
  if dataEntry.streamingRequest:
    discard # Fall through to streaming body handling below
  elif not dataEntry.requestState.headersParsed:
    # Not done with headers yet, look for the end of the headers
    let headersEnd = dataEntry.recvBuf.find(
      "\r\n\r\n",
      0,
      min(dataEntry.bytesReceived, server.maxHeadersLen) - 1 # Inclusive
    )
    if headersEnd < 0: # Headers end not found
      if dataEntry.bytesReceived > server.maxHeadersLen:
        server.log(DebugLevel, "Dropped connection, headers too long")
        return true # Headers too long or malformed, close the connection
      return false # Try again after receiving more bytes

    # We have the headers, now to parse them (avoiding excess allocations)

    var lineNum, lineStart: int
    while lineStart < headersEnd:
      var lineEnd = dataEntry.recvBuf.find(
        "\r\n",
        lineStart,
        headersEnd
      )
      if lineEnd == -1:
        lineEnd = headersEnd

      var lineLen = lineEnd - lineStart
      while lineLen > 0 and dataEntry.recvBuf[lineStart] in whitespace:
        inc lineStart
        dec lineLen
      while lineLen > 0 and
        dataEntry.recvBuf[lineStart + lineLen - 1] in whitespace:
        dec lineLen

      if lineNum == 0: # This is the request line
        let space1 = dataEntry.recvBuf.find(
          ' ',
          lineStart,
          lineStart + lineLen - 1
        )
        if space1 == -1:
          return true # Invalid request line, close the connection
        dataEntry.requestState.httpMethod = dataEntry.recvBuf[lineStart ..< space1]
        let
          remainingLen = lineLen - (space1 + 1 - lineStart)
          space2 = dataEntry.recvBuf.find(
            ' ',
            space1 + 1,
            space1 + 1 + remainingLen - 1
          )
        if space2 == -1:
          return true # Invalid request line, close the connection
        dataEntry.requestState.uri = dataEntry.recvBuf[space1 + 1 ..< space2]
        try:
          var url = parseUrl(dataEntry.requestState.uri)
          dataEntry.requestState.path = move url.path
          dataEntry.requestState.queryParams = move url.query
        except Exception as e:
          server.log(
            DebugLevel,
            "Dropped connection, invalid request URI: " &
            dataEntry.requestState.uri
          )
          return true # Invalid request URI, close the connection
        if dataEntry.recvBuf.find(
          ' ',
          space2 + 1,
          lineStart + lineLen - 1
        ) != -1:
          return true # Invalid request line, close the connection
        let httpVersionLen = lineLen - (space2 + 1 - lineStart)
        if httpVersionLen != 8:
          return true # Invalid request line, close the connection
        {.gcsafe.}:
          if equalMem(
            dataEntry.recvBuf[space2 + 1].addr,
            http11[0].unsafeAddr,
            8
          ):
            dataEntry.requestState.httpVersion = Http11
          elif equalMem(
            dataEntry.recvBuf[space2 + 1].addr,
            http10[0].unsafeAddr,
            8
          ):
            dataEntry.requestState.httpVersion = Http10
          else:
            return true # Unsupported HTTP version, close the connection
      else: # This is a header
        let splitAt = dataEntry.recvBuf.find(
          ':',
          lineStart,
          lineStart + lineLen - 1
        )
        if splitAt == -1:
          # Malformed header, include it for debugging purposes
          var line = dataEntry.recvBuf[lineStart ..< lineStart + lineLen]
          dataEntry.requestState.headers.add((move line, ""))
        else:
          var
            leftStart = lineStart
            leftLen = splitAt - leftStart
            rightStart = splitAt + 1
            rightLen = lineStart + lineLen - rightStart

          while leftLen > 0 and
            dataEntry.recvBuf[leftStart] in whitespace:
            inc leftStart
            dec leftLen
          while leftLen > 0 and
            dataEntry.recvBuf[leftStart + leftLen - 1] in whitespace:
            dec leftLen
          while rightLen > 0 and
            dataEntry.recvBuf[rightStart] in whitespace:
            inc rightStart
            dec rightLen
          while leftLen > 0 and
            dataEntry.recvBuf[rightStart + rightLen - 1] in whitespace:
            dec rightLen

          # TODO: Headers must not contain control characters (0-31, 127)

          dataEntry.requestState.headers.add((
            dataEntry.recvBuf[leftStart ..< leftStart + leftLen],
            dataEntry.recvBuf[rightStart ..< rightStart + rightLen]
          ))

      lineStart = lineEnd + 2
      inc lineNum

    dataEntry.requestState.chunked =
      dataEntry.requestState.headers.headerContainsToken(
        "Transfer-Encoding", "chunked"
      )

    var foundContentLength, foundTransferEncoding: bool
    for (k, v) in dataEntry.requestState.headers:
      if cmpIgnoreCase(k, "Content-Length") == 0:
        if foundContentLength:
          # This is a second Content-Length header, not valid
          return true # Close the connection
        foundContentLength = true
        if dataEntry.requestState.chunked:
          # Found both Transfer-Encoding: chunked and Content-Length headers
          return true # Close the connection
        try:
          dataEntry.requestState.contentLength = strictParseInt(v)
        except Exception as e:
          return true # Parsing Content-Length failed, close the connection
      elif cmpIgnoreCase(k, "Transfer-Encoding") == 0:
        if foundTransferEncoding:
          # This is a second Transfer-Encoding header, not valid
          return true # Close the connection
        foundTransferEncoding = true

    if dataEntry.requestState.contentLength < 0:
      return true # Invalid Content-Length, close the connection

    # Remove the headers from the receive buffer
    # We do this so we can hopefully just move the receive buffer at the end
    # instead of always copying a potentially huge body
    let bodyStart = headersEnd + 4
    if dataEntry.bytesReceived == bodyStart:
      dataEntry.bytesReceived = 0
    else:
      # This could be optimized away by having [0] be [head] where head can move
      # without having to copy the headers out
      # Preferring to copy the headers out to avoid the worst case of copying
      # huge bodies
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[bodyStart].addr,
        dataEntry.bytesReceived - bodyStart
      )
      dataEntry.bytesReceived -= bodyStart

    # One of three possible states for request body:
    # 1) We received a Content-Length header, so we know the content length
    # 2) We received a Transfer-Encoding: chunked header
    # 3) Neither, so we assume a content length of 0

    # Mark that headers have been parsed, must end this block
    dataEntry.requestState.headersParsed = true

    # Check if this request should be streamed
    if server.streamingThreshold >= 0 and (
      dataEntry.requestState.chunked or
      dataEntry.requestState.contentLength > server.streamingThreshold
    ):
      let channel = initStreamChannel()
      dataEntry.streamChannel = channel
      dataEntry.streamingRequest = true
      dataEntry.streamChunked = dataEntry.requestState.chunked
      dataEntry.streamBytesRemaining =
        dataEntry.requestState.contentLength

      let reqStream = cast[RequestStream](
        allocShared0(sizeof(RequestStreamObj))
      )
      reqStream.server = server
      reqStream.clientSocket = clientSocket
      reqStream.clientId = dataEntry.clientId
      reqStream.channel = channel
      reqStream.contentLength =
        if dataEntry.requestState.chunked: -1
        else: dataEntry.requestState.contentLength

      # Pop the request immediately (with empty body) and dispatch
      let request = server.popRequest(clientSocket, dataEntry)
      request.requestStream = reqStream
      server.postTask(WorkerTask(request: request))
      # Fall through to push any buffered body data below

  # Headers have been parsed, now for the body

  # If this is a streaming request, push body data to the channel
  if dataEntry.streamingRequest:
    let channel = dataEntry.streamChannel
    if dataEntry.streamChunked:
      # Streaming chunked: decode chunks and push decoded data to channel
      while true:
        if dataEntry.bytesReceived < 3:
          return false

        let chunkLenEnd = dataEntry.recvBuf.find(
          "\r\n",
          0,
          min(dataEntry.bytesReceived - 1, 19)
        )
        if chunkLenEnd < 0:
          if dataEntry.bytesReceived > 19:
            withLock channel.lock:
              channel.errorChannel()
              signal(channel.cond)
            dataEntry.streamingRequest = false
            return true
          return false

        var chunkLen: int
        try:
          chunkLen =
            strictParseHex(dataEntry.recvBuf.toOpenArray(0, chunkLenEnd - 1))
        except Exception as e:
          withLock channel.lock:
            channel.errorChannel()
            signal(channel.cond)
          dataEntry.streamingRequest = false
          return true

        let chunkStart = chunkLenEnd + 2
        if dataEntry.bytesReceived < chunkStart + chunkLen + 2:
          return false

        if chunkLen == 0:
          # End of chunked body
          let nextChunkStart = chunkLenEnd + 2 + chunkLen + 2
          let bytesRemaining = dataEntry.bytesReceived - nextChunkStart
          copyMem(
            dataEntry.recvBuf[0].addr,
            dataEntry.recvBuf[nextChunkStart].addr,
            bytesRemaining
          )
          dataEntry.bytesReceived = bytesRemaining
          withLock channel.lock:
            channel.closeChannel()
            signal(channel.cond)
          dataEntry.streamingRequest = false
          dataEntry.streamChannel = nil
          return false

        # Push chunk data to channel
        var written = 0
        withLock channel.lock:
          written = channel.writeChannel(
            dataEntry.recvBuf[chunkStart].addr,
            chunkLen
          )
          if written > 0:
            signal(channel.cond)

        if written < chunkLen:
          # Channel is full, pause reading from this socket
          withLock channel.lock:
            channel.pausedReading = true
          let events =
            if dataEntry.outgoingBuffers.len > 0: {Write}
            else: {} # No events - paused
          server.selector.updateHandle2(clientSocket, events)
          return false

        # Remove this chunk from the receive buffer
        let
          nextChunkStart = chunkLenEnd + 2 + chunkLen + 2
          bytesRemaining = dataEntry.bytesReceived - nextChunkStart
        copyMem(
          dataEntry.recvBuf[0].addr,
          dataEntry.recvBuf[nextChunkStart].addr,
          bytesRemaining
        )
        dataEntry.bytesReceived = bytesRemaining
    else:
      # Streaming Content-Length: push raw bytes to channel
      while dataEntry.bytesReceived > 0 and
          dataEntry.streamBytesRemaining > 0:
        let toWrite = min(dataEntry.bytesReceived,
                          dataEntry.streamBytesRemaining)
        var written: int
        var paused = false
        withLock channel.lock:
          written = channel.writeChannel(
            dataEntry.recvBuf[0].addr,
            toWrite
          )
          if written > 0:
            signal(channel.cond)
            dataEntry.streamBytesRemaining -= written
          if written == 0 or channel.freeSpace == 0:
            # Mark paused inside the lock so the worker thread sees it
            channel.pausedReading = true
            paused = true

        if written > 0:
          if written < dataEntry.bytesReceived:
            copyMem(
              dataEntry.recvBuf[0].addr,
              dataEntry.recvBuf[written].addr,
              dataEntry.bytesReceived - written
            )
          dataEntry.bytesReceived -= written

        if paused:
          # Remove Read from selector to stop receiving
          let events =
            if dataEntry.outgoingBuffers.len > 0: {Write}
            else: {} # No events - paused
          server.selector.updateHandle2(clientSocket, events)
          return false

      if dataEntry.streamBytesRemaining <= 0:
        # All body bytes have been delivered
        withLock channel.lock:
          channel.closeChannel()
          signal(channel.cond)
        dataEntry.streamingRequest = false
        dataEntry.streamChannel = nil
      return false
  elif dataEntry.requestState.chunked: # Chunked request (non-streaming)
    # Process as many chunks as we have
    while true:
      if dataEntry.bytesReceived < 3:
        return false # Need to receive more bytes

      # Look for the end of the chunk length
      let chunkLenEnd = dataEntry.recvBuf.find(
        "\r\n",
        0,
        min(dataEntry.bytesReceived - 1, 19) # Inclusive with a reasonable max
      )
      if chunkLenEnd < 0: # Chunk length end not found
        if dataEntry.bytesReceived > 19:
          return true # We should have found it, close the connection
        return false # Try again after receiving more bytes

      # After we know we've seen the end of the chunk length, parse it
      var chunkLen: int
      try:
        chunkLen =
          strictParseHex(dataEntry.recvBuf.toOpenArray(0, chunkLenEnd - 1))
      except Exception as e:
        return true # Parsing chunk length failed, close the connection

      if dataEntry.requestState.contentLength + chunkLen > server.maxBodyLen:
        server.log(DebugLevel, "Dropped connection, body too long")
        return true # Body is too large, close the connection

      let chunkStart = chunkLenEnd + 2
      if dataEntry.bytesReceived < chunkStart + chunkLen + 2:
        return false # Need to receive more bytes

      # Make room in the body buffer for this chunk
      let newContentLength = dataEntry.requestState.contentLength + chunkLen
      if dataEntry.requestState.body.len < newContentLength:
        let newLen = max(dataEntry.requestState.body.len * 2, newContentLength)
        dataEntry.requestState.body.setLen(newLen)

      if chunkLen > 0:
        copyMem(
          dataEntry.requestState.body[dataEntry.requestState.contentLength].addr,
          dataEntry.recvBuf[chunkStart].addr,
          chunkLen
        )
        dataEntry.requestState.contentLength += chunkLen

      # Remove this chunk from the receive buffer
      let
        nextChunkStart = chunkLenEnd + 2 + chunkLen + 2
        bytesRemaining = dataEntry.bytesReceived - nextChunkStart
      copyMem(
        dataEntry.recvBuf[0].addr,
        dataEntry.recvBuf[nextChunkStart].addr,
        bytesRemaining
      )
      dataEntry.bytesReceived = bytesRemaining

      if chunkLen == 0: # A chunk of len 0 marks the end of the request body
        let request = server.popRequest(clientSocket, dataEntry)
        server.postTask(WorkerTask(request: request))
  else:
    if dataEntry.requestState.contentLength > server.maxBodyLen:
      server.log(DebugLevel, "Dropped connection, body too long")
      return true # Body is too large, close the connection

    if dataEntry.bytesReceived < dataEntry.requestState.contentLength:
      return false # Need to receive more bytes

    # We have the entire request body

    # If this request has a body
    if dataEntry.requestState.contentLength > 0:
      # If the receive buffer only has the body in it, just move it and reset
      # the receive buffer
      if dataEntry.requestState.contentLength == dataEntry.bytesReceived:
        dataEntry.requestState.body = move dataEntry.recvBuf
        dataEntry.recvBuf.setLen(initialRecvBufLen)
        dataEntry.bytesReceived = 0
      else:
        # Copy the body out of the buffer
        dataEntry.requestState.body.setLen(dataEntry.requestState.contentLength)
        copyMem(
          dataEntry.requestState.body[0].addr,
          dataEntry.recvBuf[0].addr,
          dataEntry.requestState.contentLength
        )
        # Remove this request from the receive buffer
        let bytesRemaining =
          dataEntry.bytesReceived - dataEntry.requestState.contentLength
        copyMem(
          dataEntry.recvBuf[0].addr,
          dataEntry.recvBuf[dataEntry.requestState.contentLength].addr,
          bytesRemaining
        )
        dataEntry.bytesReceived = bytesRemaining

    let request = server.popRequest(clientSocket, dataEntry)
    server.postTask(WorkerTask(request: request))

proc afterRecv(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  # Have we upgraded this connection to a websocket?
  # If not, treat incoming bytes as part of HTTP requests.
  if dataEntry.upgradedToWebSocket:
    server.afterRecvWebSocket(clientSocket, dataEntry)
  else:
    server.afterRecvHttp(clientSocket, dataEntry)

proc afterSend(
  server: Server,
  clientSocket: SocketHandle,
  dataEntry: DataEntry
): bool {.raises: [IOSelectorsException].} =
  let
    outgoingBuffer = dataEntry.outgoingBuffers.peekFirst()
    totalBytes = outgoingBuffer.buffer1.len + outgoingBuffer.buffer2.len
  if outgoingBuffer.bytesSent == totalBytes:
    # The current outgoing buffer for this socket has been fully sent
    # Remove it from the outgoing buffer queue
    dataEntry.outgoingBuffers.shrink(fromFirst = 1)
    # Signal response stream backpressure if applicable
    if outgoingBuffer.responseStream != nil:
      let stream = outgoingBuffer.responseStream
      let prev = stream.pendingBytes.fetchSub(totalBytes)
      if prev >= stream.maxPendingBytes and
          prev - totalBytes < stream.maxPendingBytes:
        withLock stream.lock:
          signal(stream.cond)
    if outgoingBuffer.isCloseFrame:
      dataEntry.closeFrameSent = true
    if outgoingBuffer.closeConnection:
      return true
  # If we don't have any more outgoing buffers, update the selector
  if dataEntry.outgoingBuffers.len == 0:
    server.selector.updateHandle2(clientSocket, {Read})

proc destroy(server: Server, joinThreads: bool) {.raises: [].} =
  withLock server.taskQueueLock:
    server.destroyCalled = true
  if server.selector != nil:
    try:
      server.selector.close()
    except Exception as e:
      discard # Ignore
  if server.socket.int != 0:
    server.socket.close()
  for clientSocket in server.clientSockets:
    clientSocket.close()
  broadcast(server.taskQueueCond)
  if joinThreads:
    joinThreads(server.workerThreads)
    deinitLock(server.taskQueueLock)
    deinitCond(server.taskQueueCond)
    deinitLock(server.responseQueueLock)
    deinitLock(server.sendQueueLock)
    deinitLock(server.websocketQueuesLock)
    deinitLock(server.streamResumeQueueLock)
    try:
      server.responseQueued.close()
    except Exception as e:
      discard # Ignore
    try:
      server.sendQueued.close()
    except Exception as e:
      discard # Ignore
    try:
      server.shutdown.close()
    except Exception as e:
      discard # Ignore
    try:
      server.streamResumeReading.close()
    except Exception as e:
      discard # Ignore
    `=destroy`(server[])
    deallocShared(server)
  else:
    # This is not a clean exit, leak to avoid potential segfaults for now
    # The process is likely going to be exiting anyway
    discard

proc loopForever(server: Server) {.raises: [OSError, IOSelectorsException].} =
  var
    readyKeys: array[maxEventsPerSelectLoop, ReadyKey]
    receivedFrom, sentTo: seq[SocketHandle]
    needClosing: HashSet[SocketHandle]
    encodedResponses: seq[OutgoingBuffer]
    encodedFrames: seq[OutgoingBuffer]
  while true:
    receivedFrom.setLen(0)
    sentTo.setLen(0)
    needClosing.clear()
    encodedResponses.setLen(0)
    encodedFrames.setLen(0)

    let readyCount = server.selector.selectInto(-1, readyKeys)

    # Collapse these events into simple flags
    var
      responseQueuedTriggered, sendQueuedTriggered: bool
      shutdownTriggered, streamResumeTriggered: bool
    for i in 0 ..< readyCount:
      let readyKey = readyKeys[i]
      if User in readyKey.events:
        let eventDataEntry = server.selector.getData(readyKey.fd)
        if eventDataEntry.event == server.responseQueued:
          responseQueuedTriggered = true
        if eventDataEntry.event == server.sendQueued:
          sendQueuedTriggered = true
        elif eventDataEntry.event == server.shutdown:
          shutdownTriggered = true
        elif eventDataEntry.event == server.streamResumeReading:
          streamResumeTriggered = true
        else:
          discard

    if responseQueuedTriggered:
      # If we have responses queued move them to the outgoing buffer queue of
      # the appropriate socket and update the socket selector to include Write

      withLock server.responseQueueLock:
        while server.responseQueue.len > 0:
          encodedResponses.add(server.responseQueue.popFirst())

      for encodedResponse in encodedResponses:
        if encodedResponse.clientSocket in server.selector:
          let clientDataEntry =
            server.selector.getData(encodedResponse.clientSocket)
          if encodedResponse.clientId == clientDataEntry.clientId:
            clientDataEntry.outgoingBuffers.addLast(encodedResponse)
            server.selector.updateHandle2(
              encodedResponse.clientSocket,
              {Read, Write}
            )

            clientDataEntry.requestCounter =
              max(clientDataEntry.requestCounter - 1, 0)

            if encodedResponse.isWebSocketUpgrade:
              clientDataEntry.upgradedToWebSocket = true
              let websocket = WebSocket(
                server: server,
                clientSocket: encodedResponse.clientSocket,
                clientId: encodedResponse.clientId
              )
              withLock server.websocketQueuesLock:
                server.websocketQueues[websocket] = initDeque[WebSocketUpdate]()
                server.websocketClaimed[websocket] = false
              websocket.postWebSocketUpdate(WebSocketUpdate(event: OpenEvent))
              # Are there any sends that were waiting for this response?
              if clientDataEntry.sendsWaitingForUpgrade.len > 0:
                for encodedFrame in clientDataEntry.sendsWaitingForUpgrade:
                  if clientDataEntry.closeFrameQueuedAt > 0:
                    server.log(DebugLevel, "Dropped message after WebSocket close")
                  else:
                    clientDataEntry.outgoingBuffers.addLast(encodedFrame)
                    if encodedFrame.isCloseFrame:
                      clientDataEntry.closeFrameQueuedAt = epochTime()
                clientDataEntry.sendsWaitingForUpgrade.setLen(0)
          else:
            # Was this file descriptor reused for a different client?
            server.log(DebugLevel, "Dropped response to disconnected client")
        else:
          server.log(DebugLevel, "Dropped response to disconnected client")

    if sendQueuedTriggered:
      # If we have any sends queued move them to the outgoing buffer queue of
      # the appropriate socket and update the socket selector to include Write

      withLock server.sendQueueLock:
        while server.sendQueue.len > 0:
          encodedFrames.add(server.sendQueue.popFirst())

      for encodedFrame in encodedFrames:
        if encodedFrame.clientSocket in server.selector:
          let clientDataEntry =
            server.selector.getData(encodedFrame.clientSocket)
          if encodedFrame.clientId == clientDataEntry.clientId:
            # Have we sent the upgrade response yet?
            if clientDataEntry.upgradedToWebSocket:
              if clientDataEntry.closeFrameQueuedAt > 0:
                server.log(DebugLevel, "Dropped message after WebSocket close")
              else:
                clientDataEntry.outgoingBuffers.addLast(encodedFrame)
                if encodedFrame.isCloseFrame:
                  clientDataEntry.closeFrameQueuedAt = epochTime()
                server.selector.updateHandle2(
                  encodedFrame.clientSocket,
                  {Read, Write}
                )
            else:
              # If we haven't, queue this to wait for the upgrade response
              clientDataEntry.sendsWaitingForUpgrade.add(encodedFrame)
          else:
            # Was this file descriptor reused for a different client?
            server.log(DebugLevel, "Dropped message to disconnected client")
        else:
          server.log(DebugLevel, "Dropped message to disconnected client")

    if streamResumeTriggered:
      var socketsToResume: seq[SocketHandle]
      withLock server.streamResumeQueueLock:
        while server.streamResumeQueue.len > 0:
          socketsToResume.add(server.streamResumeQueue.popFirst())

      for clientSocket in socketsToResume:
        if clientSocket in server.selector:
          let dataEntry = server.selector.getData(clientSocket)
          if dataEntry.streamChannel != nil:
            let events =
              if dataEntry.outgoingBuffers.len > 0: {Read, Write}
              else: {Read}
            server.selector.updateHandle2(clientSocket, events)
            # Process any buffered data that was received before pausing
            if dataEntry.bytesReceived > 0:
              receivedFrom.add(clientSocket)

    if shutdownTriggered:
      server.destroy(true)
      return

    # This is the main client socket select loop
    for i in 0 ..< readyCount:
      let readyKey = readyKeys[i]

      # echo "Socket ready: ", readyKey.fd, " ", readyKey.events

      if readyKey.fd == server.socket.int:
        # We should have a new client socket to accept
        if Read in readyKey.events:
          let (clientSocket, remoteAddress) =
            when defined(linux) and not defined(nimdoc):
              var
                sockAddr: SockAddr
                addrLen = sizeof(sockAddr).SockLen
              let
                socket =
                  accept4(
                    server.socket,
                    sockAddr.addr,
                    addrLen.addr,
                    SOCK_CLOEXEC or SOCK_NONBLOCK
                  )
                sockAddrStr =
                  try:
                    getAddrString(sockAddr.addr)
                  except Exception as e:
                    ""
              (socket, sockAddrStr)
            else:
              server.socket.accept()

          if clientSocket == osInvalidSocket:
            continue

          when not defined(linux):
            # Not needed on linux where we can use SOCK_NONBLOCK
            clientSocket.setBlocking(false)

          server.clientSockets.incl(clientSocket)

          let dataEntry = DataEntry(kind: ClientSocketEntry)
          dataEntry.clientId = server.rand.next()
          dataEntry.remoteAddress = remoteAddress
          dataEntry.recvBuf.setLen(initialRecvBufLen)
          server.selector.registerHandle2(clientSocket, {Read}, dataEntry)
      else: # Client socket
        if Error in readyKey.events:
          needClosing.incl(readyKey.fd.SocketHandle)
          continue

        let dataEntry = server.selector.getData(readyKey.fd)

        if Read in readyKey.events:
          # Expand the buffer if it is full
          if dataEntry.bytesReceived == dataEntry.recvBuf.len:
            dataEntry.recvBuf.setLen(dataEntry.recvBuf.len * 2)

          let bytesReceived = readyKey.fd.SocketHandle.recv(
            dataEntry.recvBuf[dataEntry.bytesReceived].addr,
            (dataEntry.recvBuf.len - dataEntry.bytesReceived).cint,
            0
          )
          if bytesReceived > 0:
            dataEntry.bytesReceived += bytesReceived
            receivedFrom.add(readyKey.fd.SocketHandle)
          else:
            needClosing.incl(readyKey.fd.SocketHandle)
            continue

        if Write in readyKey.events:
          let outgoingBuffer = dataEntry.outgoingBuffers.peekFirst()
          let totalBytes =
            outgoingBuffer.buffer1.len + outgoingBuffer.buffer2.len
          if totalBytes == 0:
            # Empty buffer (e.g. HTTP/1.0 streaming finish), process directly
            sentTo.add(readyKey.fd.SocketHandle)
          else:
            let bytesSent =
              if outgoingBuffer.bytesSent < outgoingBuffer.buffer1.len:
                readyKey.fd.SocketHandle.send(
                  outgoingBuffer.buffer1[outgoingBuffer.bytesSent].addr,
                  (outgoingBuffer.buffer1.len - outgoingBuffer.bytesSent).cint,
                  when defined(MSG_NOSIGNAL): MSG_NOSIGNAL else: 0
                )
              else:
                let buffer2Pos =
                  outgoingBuffer.bytesSent - outgoingBuffer.buffer1.len
                readyKey.fd.SocketHandle.send(
                  outgoingBuffer.buffer2[buffer2Pos].addr,
                  (outgoingBuffer.buffer2.len - buffer2Pos).cint,
                  when defined(MSG_NOSIGNAL): MSG_NOSIGNAL else: 0
                )
            if bytesSent > 0:
              outgoingBuffer.bytesSent += bytesSent
              sentTo.add(readyKey.fd.SocketHandle)
            else:
              needClosing.incl(readyKey.fd.SocketHandle)
              continue

    for clientSocket in receivedFrom:
      if clientSocket in needClosing:
        continue
      let
        dataEntry = server.selector.getData(clientSocket)
        needsClosing = server.afterRecv(clientSocket, dataEntry)
      if needsClosing:
        needClosing.incl(clientSocket)

    for clientSocket in sentTo:
      if clientSocket in needClosing:
        continue
      let
        dataEntry = server.selector.getData(clientSocket)
        needsClosing = server.afterSend(clientSocket, dataEntry)
      if needsClosing:
        needClosing.incl(clientSocket)

    for clientSocket in needClosing:
      let dataEntry = server.selector.getData(clientSocket)
      try:
        server.selector.unregister(clientSocket)
      except Exception as e:
        # Leaks DataEntry for this socket
        server.log(DebugLevel, "Error unregistering client socket")
      finally:
        clientSocket.close()
        server.clientSockets.excl(clientSocket)
      # Signal error on streaming upload channel
      if dataEntry.streamChannel != nil:
        withLock dataEntry.streamChannel.lock:
          dataEntry.streamChannel.errorChannel()
          signal(dataEntry.streamChannel.cond)
        dataEntry.streamingRequest = false
        dataEntry.streamChannel = nil
      # Signal error on any active response streams
      for outgoing in dataEntry.outgoingBuffers:
        if outgoing.responseStream != nil:
          outgoing.responseStream.error.store(true, moRelaxed)
          withLock outgoing.responseStream.lock:
            signal(outgoing.responseStream.cond)
      if dataEntry.upgradedToWebSocket:
        let websocket = WebSocket(
          server: server,
          clientSocket: clientSocket,
          clientId: dataEntry.clientId
        )
        if not dataEntry.closeFrameSent:
          var error = WebSocketUpdate(event: ErrorEvent)
          websocket.postWebSocketUpdate(error)
        var close = WebSocketUpdate(event: CloseEvent)
        websocket.postWebSocketUpdate(close)

proc close*(server: Server) {.raises: [], gcsafe.} =
  ## Cleanly stops and deallocates the server.
  ## In-flight request handler calls will be allowed to finish.
  ## No additional handler calls will be dispatched even if they are queued.
  if server.socket.int != 0:
    server.trigger(server.shutdown)
  else:
    server.destroy(true)

proc serve*(
  server: Server,
  port: Port,
  address = "localhost"
) {.raises: [MummyError].} =
  ## The server will serve on the address and port. The default address is
  ## localhost. Use "0.0.0.0" to make the server externally accessible (with
  ## caution).
  ## This call does not return unless server.close() is called from another
  ## thread.

  if server.socket.int != 0:
    raise newException(MummyError, "Server already has a socket")

  try:
    server.socket = createNativeSocket(
      Domain.AF_INET,
      SockType.SOCK_STREAM,
      Protocol.IPPROTO_TCP,
      false
    )
    if server.socket == osInvalidSocket:
      raiseOSError(osLastError())

    server.socket.setBlocking(false)
    server.socket.setSockOptInt(SOL_SOCKET, SO_REUSEADDR, 1)

    let ai = getAddrInfo(
      address,
      port,
      Domain.AF_INET,
      SockType.SOCK_STREAM,
      Protocol.IPPROTO_TCP,
    )
    try:
      if bindAddr(server.socket, ai.ai_addr, ai.ai_addrlen.SockLen) < 0:
        raiseOSError(osLastError())
    finally:
      freeAddrInfo(ai)

    if nativesockets.listen(server.socket, listenBacklogLen) < 0:
      raiseOSError(osLastError())

    let dataEntry = DataEntry(kind: ServerSocketEntry)
    server.selector.registerHandle2(server.socket, {Read}, dataEntry)
  except Exception as e:
    server.destroy(true)
    raise currentExceptionAsMummyError()

  server.serving.store(true, moRelaxed)

  try:
    server.loopForever()
  except Exception as e:
    server.log(ErrorLevel, e.msg & "\n" & e.getStackTrace())
    server.destroy(false)
    raise currentExceptionAsMummyError()

proc newServer*(
  handler: RequestHandler,
  websocketHandler: WebSocketHandler = nil,
  logHandler: LogHandler = nil,
  workerThreads = max(countProcessors() * 10, 1),
  maxHeadersLen = 8 * 1024, # 8 KB
  maxBodyLen = 1024 * 1024, # 1 MB
  maxMessageLen = 64 * 1024, # 64 KB
  streamingThreshold = -1 # Body size above which requests are streamed (-1 = never)
): Server {.raises: [MummyError].} =
  ## Creates a new HTTP server. The request handler will be called for incoming
  ## HTTP requests. The WebSocket handler will be called for WebSocket events.
  ## Calls to the HTTP, WebSocket and log handlers are made from worker threads.
  ## WebSocket events are dispatched serially per connection. This means your
  ## WebSocket handler must return from a call before the next call will be
  ## dispatched for the same connection.
  ## Set streamingThreshold >= 0 to enable streaming request bodies. Requests
  ## with Content-Length exceeding this value (or chunked requests when threshold
  ## is >= 0) will have their body streamed via request.read() instead of
  ## being fully buffered in request.body.

  if handler == nil:
    raise newException(MummyError, "The request handler must not be nil")

  var workerThreads = workerThreads
  when defined(mummyNoWorkers): # For testing, fuzzing etc
    workerThreads = 0

  result = cast[Server](allocShared0(sizeof(ServerObj)))
  result.handler = handler
  result.websocketHandler = websocketHandler
  result.logHandler = if logHandler != nil: logHandler else: echoLogger
  result.maxHeadersLen = maxHeadersLen
  result.maxBodyLen = maxBodyLen
  result.maxMessageLen = maxMessageLen
  result.streamingThreshold = streamingThreshold
  result.rand = initRand()

  result.workerThreads.setLen(workerThreads)

  # Stuff that can fail
  try:
    result.responseQueued = newSelectEvent()
    result.sendQueued = newSelectEvent()
    result.shutdown = newSelectEvent()
    result.streamResumeReading = newSelectEvent()

    result.selector = newSelector[DataEntry]()

    let responseQueuedData = DataEntry(kind: EventEntry)
    responseQueuedData.event = result.responseQueued
    result.selector.registerEvent(result.responseQueued, responseQueuedData)

    let sendQueuedData = DataEntry(kind: EventEntry)
    sendQueuedData.event = result.sendQueued
    result.selector.registerEvent(result.sendQueued, sendQueuedData)

    let shutdownData = DataEntry(kind: EventEntry)
    shutdownData.event = result.shutdown
    result.selector.registerEvent(result.shutdown, shutdownData)

    let streamResumeData = DataEntry(kind: EventEntry)
    streamResumeData.event = result.streamResumeReading
    result.selector.registerEvent(result.streamResumeReading, streamResumeData)

    initLock(result.taskQueueLock)
    initCond(result.taskQueueCond)
    initLock(result.responseQueueLock)
    initLock(result.sendQueueLock)
    initLock(result.websocketQueuesLock)
    initLock(result.streamResumeQueueLock)

    for i in 0 ..< workerThreads:
      createThread(result.workerThreads[i], workerProc, result)
  except Exception as e:
    result.destroy(true)
    raise currentExceptionAsMummyError()

proc responded*(request: Request): bool =
  ## Check if this request has been responded.
  ## Informational responses (1xx status codes) do not mark a request responded.
  # This is only safe to call on the request handler thread right now, improve?
  request.responded

proc waitUntilReady*(server: Server, timeout: float = 10) =
  ## This proc blocks until the server is ready to receive requests or
  ## the timeout has passed. The timeout is in floating point seconds.
  ## This is useful when writing tests, where you need to know
  ## the server is ready before you begin sending requests.
  ## If the server is already ready this returns immediately.
  let start = cpuTime()
  while true:
    if server.serving.load(moRelaxed):
      return
    let
      now = cpuTime()
      delta = now - start
    if delta > timeout:
      raise newException(MummyError, "Timeout while waiting for server")
    sleep(100)
