Spring stream interoperability
==============================

This project offers two integrations:

1. A `ClientHttpRequestFactory` for the JDK HTTP client to work with `RestTemplate`: while rest template is not (no longer) officially deprecated, its development has become dormant. If the `RestTemplate` should be used alongside Spring's `WebClient`, this connector allows to use the same underlying client for both APIs.
2. Decoders and body inserters/extractors for streams: The `InputStreamDecoder` allows to map the result of a request through the `WebClient` to an `InputStream` that makes bytes available as they arrive. For this, it must be registered as a codec in the web client's builder, and the stream must be closed to avoid leaking data. Without a codec, it offers similar integrations to read bodys as streams via streams using `StreamBodyExtractors`. And body's can be written to a request using `StreamBodyInserters`.
