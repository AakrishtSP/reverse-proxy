-- Chunked request-body upload workload for wrk.
-- Sends one 16-byte data chunk plus the terminal chunk, Transfer-Encoding:
-- chunked, so the proxy's chunked scanner path is exercised. The raw request
-- string keeps wrk from injecting a Content-Length (which would fight TE).
wrk.method = "POST"

local body = "10\r\n0123456789abcdef\r\n0\r\n\r\n"

function wrk.request()
    return "POST / HTTP/1.1\r\nHost: bench\r\nTransfer-Encoding: chunked\r\n\r\n" .. body
end