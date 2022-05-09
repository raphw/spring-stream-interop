package codes.rafael.springstreaminterop.webflux;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.ReactiveHttpOutputMessage;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.BodyInserter;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * An extension of {@link org.springframework.web.reactive.function.BodyInserters} for
 * stream interoperability.
 */
public class StreamBodyInserters {

    /**
     * Inserter where the request body is written to an output stream. The stream is closed
     * automatically if it is not closed manually.
     * @param consumer the consumer that is writing to the output stream
     * @return the inserter to write directly to the body via an output stream
     */
    public static BodyInserter<Void, ReactiveHttpOutputMessage> fromOutputStream(
            FromOutputStream consumer) {

        Assert.notNull(consumer, "'publisher' must not be null");
        return (outputMessage, context) -> {
            Sinks.Many<DataBuffer> sink = Sinks.many()
                    .unicast()
                    .onBackpressureBuffer();

            Mono<Void> mono = outputMessage.writeWith(sink.asFlux());
            WriterOutputStream outputStream = new WriterOutputStream(outputMessage.bufferFactory(), sink);
            try {
                consumer.accept(outputStream);
            } catch (Throwable t) {
                sink.emitError(t, Sinks.EmitFailureHandler.FAIL_FAST);
            }
            outputStream.close();

            return mono;
        };
    }

    /**
     * Variant of {@link StreamBodyInserters#fromInputStream(Supplier, int)} that uses
     * a default chunk size.
     * @param streamSupplier the supplier that is supplying the stream to write
     * @return the inserter to write the request body from a supplied input stream
     */
    public static BodyInserter<Void, ReactiveHttpOutputMessage> fromInputStream(
            Supplier<? extends InputStream> streamSupplier) {
        return fromInputStream(streamSupplier, 8192);
    }

    /**
     * Inserter where the request body is read from an input stream. The supplied
     * input stream is closed once it is no longer consumed.
     * @param streamSupplier the supplier that is supplying the stream to write
     * @param chunkSize the size of each chunk that is buffered before sending
     * @return the inserter to write the request body from a supplied input stream
     */
    public static BodyInserter<Void, ReactiveHttpOutputMessage> fromInputStream(
            Supplier<? extends InputStream> streamSupplier, int chunkSize) {

        Assert.notNull(streamSupplier, "'streamSupplier' must not be null");
        Assert.state(chunkSize > 0, "'chunkSize' must be a positive number");
        return (outputMessage, context) -> {
            Sinks.Many<DataBuffer> sink = Sinks.many()
                    .unicast()
                    .onBackpressureBuffer();

            DataBufferFactory factory = outputMessage.bufferFactory();
            Mono<Void> mono = outputMessage.writeWith(sink.asFlux());
            try {
                InputStream inputStream = streamSupplier.get();
                if (inputStream == null) {
                    sink.emitError(new NullPointerException("inputStream"), Sinks.EmitFailureHandler.FAIL_FAST);
                } else {
                    try (inputStream) {
                        int length;
                        byte[] buffer = new byte[chunkSize];
                        while ((length = inputStream.read(buffer)) != -1) {
                            if (length == 0) {
                                continue;
                            }
                            byte[] transported = new byte[length];
                            System.arraycopy(buffer, 0, transported, 0, length);
                            sink.emitNext(factory.wrap(transported), Sinks.EmitFailureHandler.FAIL_FAST);
                        }
                        sink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
                    }
                }
            } catch (Throwable t) {
                sink.emitError(t, Sinks.EmitFailureHandler.FAIL_FAST);
            }

            return mono;
        };
    }

    /**
     * A consumer for an output stream of which the content is written to the request body.
     */
    @FunctionalInterface
    public interface FromOutputStream {

        /**
         * Accepts an output stream which content is written to the request body.
         * @param outputStream the output stream that represents the request body
         * @throws IOException if an I/O error occurs what aborts the request
         */
        void accept(OutputStream outputStream) throws IOException;

    }

    private static class WriterOutputStream extends OutputStream {

        private final DataBufferFactory factory;

        private final Sinks.Many<DataBuffer> sink;

        private final AtomicBoolean closed = new AtomicBoolean();

        private WriterOutputStream(DataBufferFactory factory, Sinks.Many<DataBuffer> sink) {
            this.factory = factory;
            this.sink = sink;
        }

        @Override
        public void write(int b) throws IOException {
            if (closed.get()) {
                throw new IOException("closed");
            }
            DataBuffer buffer = factory.allocateBuffer(1);
            buffer.write((byte) (b & 0xFF));
            sink.tryEmitNext(buffer).orThrow();
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (closed.get()) {
                throw new IOException("closed");
            }
            DataBuffer buffer = factory.allocateBuffer(len);
            buffer.write(b, off, len);
            sink.tryEmitNext(buffer).orThrow();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                sink.tryEmitComplete().orThrow();
            }
        }
    }
}
