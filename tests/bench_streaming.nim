## Benchmark for streaming vs non-streaming performance
## Run with: nim c --mm:arc --threads:on -d:release -r tests/bench_streaming.nim

import mummy, std/os, std/times, std/strutils, std/net

const
  numRequests = 2000
  chunkSize = 64 * 1024
  totalStreamBytes = 1024 * 1024
  numChunks = totalStreamBytes div chunkSize

var uploadBody: string
uploadBody.setLen(totalStreamBytes)
for i in 0 ..< uploadBody.len:
  uploadBody[i] = chr(ord('a') + (i mod 26))

proc sendRecv(port: int, data: string): string =
  let sock = newSocket()
  defer: sock.close()
  sock.connect("127.0.0.1", Port(port))
  sock.send(data)
  var buf = newString(2 * 1024 * 1024)
  var total = 0
  while true:
    let n = sock.recv(buf[total].addr, buf.len - total)
    if n <= 0:
      break
    total += n
    if total >= buf.len:
      buf.setLen(buf.len * 2)
  buf.setLen(total)
  return buf

# All benchmarks on one server with routing
let chunk = uploadBody[0 ..< chunkSize]

proc handler(request: Request) =
  case request.path:
  of "/respond":
    {.gcsafe.}:
      request.respond(200, body = uploadBody)
  of "/stream-down":
    let stream = request.startResponse(200)
    {.gcsafe.}:
      for i in 0 ..< numChunks:
        stream.write(chunk)
    stream.finish()
  of "/stream-up":
    var total = 0
    if request.isStreaming:
      while true:
        let data = request.read()
        if data.len == 0:
          break
        total += data.len
    else:
      total = request.body.len
    request.respond(200, body = $total)
  of "/small":
    request.respond(200, body = "Hello, World!")
  else:
    request.respond(404)

let server = newServer(handler, streamingThreshold = 0)

type ServerInfo = object
  server: Server
  port: int

proc serverThread(info: ServerInfo) {.thread.} =
  info.server.serve(Port(info.port))

var thread: Thread[ServerInfo]
createThread(thread, serverThread, ServerInfo(server: server, port: 9300))
server.waitUntilReady()

echo "Server ready on port 9300"
echo ""

# =====================================================================
echo "=== 1. Non-streaming respond() (1MB body) ==="

block:
  let start = epochTime()
  for i in 0 ..< numRequests:
    let resp = sendRecv(9300,
      "GET /respond HTTP/1.0\r\nHost: localhost\r\n\r\n")
    if resp.len < totalStreamBytes:
      echo "  ERROR: short response: ", resp.len
      break
  let elapsed = epochTime() - start
  echo "  ", numRequests, " reqs, ", formatFloat(elapsed, ffDecimal, 3), "s, ",
    formatFloat(numRequests.float / elapsed, ffDecimal, 0), " req/s, ",
    formatFloat(totalStreamBytes.float * numRequests.float / elapsed / 1048576, ffDecimal, 1), " MB/s"

# =====================================================================
echo ""
echo "=== 2. Streaming response / chunked download (1MB, 16x64KB chunks) ==="

block:
  let start = epochTime()
  for i in 0 ..< numRequests:
    let resp = sendRecv(9300,
      "GET /stream-down HTTP/1.0\r\nHost: localhost\r\n\r\n")
    if resp.len < totalStreamBytes:
      echo "  ERROR: short response: ", resp.len
      break
  let elapsed = epochTime() - start
  echo "  ", numRequests, " reqs, ", formatFloat(elapsed, ffDecimal, 3), "s, ",
    formatFloat(numRequests.float / elapsed, ffDecimal, 0), " req/s, ",
    formatFloat(totalStreamBytes.float * numRequests.float / elapsed / 1048576, ffDecimal, 1), " MB/s"

# =====================================================================
echo ""
echo "=== 3. Streaming request / upload (1MB Content-Length) ==="

block:
  let start = epochTime()
  for i in 0 ..< numRequests:
    let resp = sendRecv(9300,
      "POST /stream-up HTTP/1.1\r\nHost: localhost\r\nContent-Length: " &
      $uploadBody.len & "\r\nConnection: close\r\n\r\n" & uploadBody)
    if resp.len == 0 or "1048576" notin resp:
      echo "  ERROR on req ", i, ": ", resp[0 .. min(100, resp.len - 1)]
      break
  let elapsed = epochTime() - start
  echo "  ", numRequests, " reqs, ", formatFloat(elapsed, ffDecimal, 3), "s, ",
    formatFloat(numRequests.float / elapsed, ffDecimal, 0), " req/s, ",
    formatFloat(totalStreamBytes.float * numRequests.float / elapsed / 1048576, ffDecimal, 1), " MB/s"

# =====================================================================
echo ""
echo "=== 4. Small body (13 bytes) with streamingThreshold active ==="

block:
  let smallCount = numRequests * 4
  let start = epochTime()
  for i in 0 ..< smallCount:
    let resp = sendRecv(9300,
      "GET /small HTTP/1.0\r\nHost: localhost\r\n\r\n")
    if "Hello" notin resp:
      echo "  ERROR: unexpected response"
      break
  let elapsed = epochTime() - start
  echo "  ", smallCount, " reqs, ", formatFloat(elapsed, ffDecimal, 3), "s, ",
    formatFloat(smallCount.float / elapsed, ffDecimal, 0), " req/s"

echo ""
echo "Done."
quit(0)
