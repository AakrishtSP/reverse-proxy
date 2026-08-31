# Server upgrade plan

Project: AakrishtSP/reverse-proxy — phases as GitHub milestones, each feature a
tracking issue, closed via pull request #24.

## Issue / milestone tracking

Milestones (github.com/AakrishtSP/reverse-proxy/milestones): Phase 0-6.

Closed by PR #24 ("Implemented Phase 1-6 features", merged `a2adb2e`), via
`Closes #N` lines:

- #1 request-line parser, #2 header parser, #12 config file parser
- #4 backend TCP connect + forward, #5 response relay, #6 backend-down handling
- #11 error log, #16 epoll event loop, #17 fixed-size worker pool,
  #18 partial read/write state machine
- #19 load-test tooling, #20 benchmark run + numbers, #21 final report

Still open (not implemented in the current codebase):

- #3 malformed-request corpus + tests, #7 connect/read timeouts
- #10 access log format, #13 backend health checks, #14 graceful shutdown,
  #15 `/stats` endpoint

Remaining open issues span Phases 1-4 (corpus tests, timeouts, access log, and
the Phase 4 "intermediate" set: health checks, graceful shutdown, `/stats`);
none is on the throughput upgrade path below.

## Throughput upgrades

Goal: lift the observed ceilings. Baseline (ReleaseFast, close/open per target):

| Server | close | open (keep-alive) | Bottleneck |
|--------|-------|-------------------|------------|
| threaded (16 thr) | ~15k | ~150k | CPU, already parallel |
| epoll (1 loop) | ~20k | ~35k | single core |

Status: step 1 (epoll multi-loop) and step 2 (io_uring) are implemented,
bench-verified, and working. Step 3 (threaded polish) remains — deprioritized.

### 1. epoll: SO_REUSEPORT multi-loop — DONE

- `.loops: usize = 1` on `EpollConfig` (default 1 = single-loop behavior).
- `run()` spawns a worker; the calling thread runs the last, so `run()` blocks.
  Each `loopWorker` owns:
  - its own listen socket on `config.listen` with `SO_REUSEPORT` before bind
    (kernel hash-balances SYNs across sockets),
  - its own epoll fd,
  - `max_connections / loops` ConnPool slots and `pool_size / loops` backend
    pool cap (lock-free, no shared state).
- Trade-off: uneven 4-tuple hashing could strand slots on one loop; revisit
  only if a loop hits capacity.
- New bench config `bench/configs/14-epoll-loops-4.zon`.

**Measured (ReleaseFast, c50):**

| Server | close | open |
|--------|-------|------|
| epoll 1 loop | ~20k | ~35k |
| epoll 4 loops | ~61k | ~122k |
| threaded (16 thr) | ~15k | ~150k |

open ~3.5x and close ~3x over a single loop, closing the keep-alive gap vs
threaded.

### 2. io_uring: a third server kind — DONE

Implemented as `ServerConfig.kind = .io_uring` (not a retrofit to epoll):

- `src/io_uring/server.zig` + `IoUringConfig` (same knobs + `entries`); main.zig
  dispatch + printConfig + default_config_zon + docs.
- Single-threaded ring loop: one op in flight per connection (accept is its own
  user_data, `acceptable_magic = 0`); batch submit + reap via `copy_cqes`.
- ConnPool of 1024 fixed slots (fd acquired on accept, refused when empty).
- Backend `connect` is a single submitted op — no EPOLLOUT dance. Fresh backend
  connection per request (`Connection: close` upstream), client keep-alive
  preserved for Content-Length-framed responses by stripping the upstream
  `Connection: close` header line (`dropHeaderLine`).
- Request rebuild drops `x-forwarded-for`; Content-Length request bodies are
  forwarded (`read_body`/`send_body`); chunked requests → 415
  (`chunked_requests = false`).
- New bench config `bench/configs/15-io-uring.zon`.

**Measured (ReleaseFast, t4, this box):**

| Server | close c50 | open c50 |
|--------|-----------|----------|
| io_uring (1 loop) | ~828k | ~371k |
| io_uring (1 loop, c200) | ~190k | ~181k |
| epoll 1 loop | ~15k | ~137k |
| epoll 4 loops | ~59k | ~116k |

A single io_uring loop beats all prior servers in both modes. (Numbers move
with wrk concurrency/threads; the ordering holds.) Not yet: chunked/streaming,
backend keep-alive reuse, multi-loop SO_REUSEPORT for io_uring.

### 3. threaded: polish only (low ROI)

Deprioritized until a bench shows a need:

- `pool_threads` sizing, per-worker `SO_REUSEPORT` listeners,
  work-stealing queue (YAGNI unless contended).

## Verification

- Harness `run-all -n` across 14-epoll-loops-4, 10-epoll-both, 03-both,
  15-io-uring, open+close.
- Curl PONG + chunked-upload smoke per kind; `zig build test`.
- `ps` VSZ/RSS to confirm pool partitioning stays bounded.

## Order

1. epoll `.loops` — DONE (merged)
2. io_uring — DONE (working, in-tree)
3. threaded polish — only if a number demands it