# Live-session response compression

Large server responses are gzip-compressed before application chunking when the
browser advertises `?compression=gzip` on its WebSocket URL. The compressed bytes
are encoded as base64 and split into the existing 512-character JSON chunk
envelopes, with `encoding: "gzip"`. The browser reassembles, decompresses UTF-8
JSON through `DecompressionStream("gzip")`, then validates the normal response.
Small responses keep their original plain JSON representation.

This is separate from WebSocket `permessage-deflate`. Transport compression
reduces wire bytes after chunking; it does not reduce the `total` chunk count
checked by the application. HTTP gzip/Brotli configuration likewise does not
compress data exchanged after a WebSocket upgrade.

## Compatibility and bounds

- Existing browsers omit the capability and continue receiving plain responses.
  An oversized plain response returns one small error asking the user to refresh,
  narrow the query, or download the result.
- Browsers without native gzip decompression also use the plain protocol.
- Outgoing SQL/editor requests retain the 10,000-chunk limit.
- Gzip responses allow at most 400,000 chunks: 204,800,000 base64 characters,
  corresponding to at most 153,600,000 compressed bytes.
- Both sender and receiver limit expanded JSON to 200 MiB of UTF-8 bytes.
  Server serialization streams into bounded gzip output without constructing a
  full uncompressed JSON string for large negotiated responses.
- The browser also bounds the combined payload of incomplete responses,
  preserves response order during decompression, rejects changing chunk metadata,
  and reports a rejected message only once. Raw payloads are not logged.

The earlier 351,273-frame response estimate of 171.5 MiB described uncompressed
text at 512 characters per chunk, approximately bytes only for ASCII. It was not
a measurement of compressed network traffic. Compression ratio depends on the
actual result data; compressed and expanded sizes are distinct limits. The
browser still needs memory to parse and display expanded results.

## Validation and rollout

Run the frontend protocol/hook tests, TypeScript, lint and build checks, plus
`WebSocketMessageChunkerTest` on JDK 21. These compression tests do not require a
database. Before a production rollout, verify a full-size round trip in a real
browser and read back the built image digest and deployed revision. Keep older
tabs in the plain-protocol compatibility check and preserve the previous image
digest for rollback.

On 2026-10-01, a Chromium check using the frontend chunk assembler and native
gzip decoder reconstructed 179,851,827 bytes of synthetic JSON exactly. Its
repeated-string payload compressed to 174,884 bytes (456 chunks), with the
compression, assembly, decoding and content check completing in about 6 seconds.
This verifies the protocol at the observed response size; it does not measure
production-query compression or result-table rendering performance.
