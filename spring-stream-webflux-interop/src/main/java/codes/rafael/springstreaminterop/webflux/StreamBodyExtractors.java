package codes.rafael.springstreaminterop.webflux;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.ReactiveHttpInputMessage;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.BodyExtractor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * An extension of {@link org.springframework.web.reactive.function.BodyExtractors} for
 * stream interoperability.
 */
public class StreamBodyExtractors {

    /**
     * Variant of {@link StreamBodyExtractors#toMono(InputStreamMapper, boolean, int)} with a
     * default buffer size and fast failure.
     * @param streamMapper the mapper that is reading the body
     * @return {@code BodyExtractor} that reads the response body as input stream
     * @param <T> the type of the value that is resolved from the returned stream
     */
    public static <T> BodyExtractor<Mono<T>, ReactiveHttpInputMessage> toMono(
            InputStreamMapper<T> streamMapper) {
        return toMono(streamMapper, true);
    }

    /**
     * Variant of {@link StreamBodyExtractors#toMono(InputStreamMapper, boolean, int)} with a
     * default buffer size.
     * @param streamMapper the mapper that is reading the body
     * @param failFast {@code false} if previously read bytes are discarded upon an error
     * @return {@code BodyExtractor} that reads the response body as input stream
     * @param <T> the type of the value that is resolved from the returned stream
     */
    public static <T> BodyExtractor<Mono<T>, ReactiveHttpInputMessage> toMono(
            InputStreamMapper<T> streamMapper,
            boolean failFast) {
        return toMono(streamMapper, failFast, 256 * 1024);
    }

    /**
     * Extractor where the response body is processed by reading an input stream of the
     * response body.
     * @param streamMapper the mapper that is reading the body
     * @param failFast {@code false} if previously read bytes are discarded upon an error
     * @param maximumMemorySize the amount of memory that is buffered until reading is suspended
     * @return {@code BodyExtractor} that reads the response body as input stream
     * @param <T> the type of the value that is resolved from the returned stream
     */
    public static <T> BodyExtractor<Mono<T>, ReactiveHttpInputMessage> toMono(
            InputStreamMapper<T> streamMapper,
            boolean failFast,
            int maximumMemorySize) {

        Assert.notNull(streamMapper, "'streamMapper' must not be null");
        Assert.isTrue(maximumMemorySize > 0, "'maximumMemorySize' must be positive");
        return (inputMessage, context) -> {
            try (FlowBufferInputStream inputStream = new FlowBufferInputStream(maximumMemorySize, failFast)) {
                inputMessage.getBody().subscribe(inputStream);
                T value = streamMapper.apply(inputStream);
                return Mono.just(value);
            } catch (Throwable t) {
                return Mono.error(t);
            }
        };
    }

    /**
     * Variant of {@link StreamBodyExtractors#toMono(InputStreamMapper, boolean, int)} with a
     * default buffer size.
     * @param streamSupplier the supplier of the output stream
     * @return {@code BodyExtractor} that reads the response body as input stream
     */
    public static BodyExtractor<Mono<Void>, ReactiveHttpInputMessage> toMono(
            Supplier<? extends OutputStream> streamSupplier) {

        Assert.notNull(streamSupplier, "'streamSupplier' must not be null");
        return (inputMessage, context) -> {
            try (OutputStream outputStream = streamSupplier.get()) {
                Flux<DataBuffer> writeResult = DataBufferUtils.write(inputMessage.getBody(), outputStream);
                writeResult.blockLast();
                return Mono.empty();
            } catch (Throwable t) {
                return Mono.error(t);
            }
        };
    }

    @FunctionalInterface
    public interface InputStreamMapper<T> {

        T apply(InputStream stream) throws IOException;
    }

    static class FlowBufferInputStream extends InputStream implements Subscriber<DataBuffer> {

        private static final Object END = new Object();

        private final AtomicBoolean closed = new AtomicBoolean();

        private final BlockingQueue<Object> backlog;

        private final int maximumMemorySize;

        private final boolean failFast;

        private final AtomicInteger buffered = new AtomicInteger();

        @Nullable
        private InputStreamWithSize current = new InputStreamWithSize(0, InputStream.nullInputStream());

        @Nullable
        private Subscription subscription;

        FlowBufferInputStream(int maximumMemorySize, boolean failFast) {
            this.backlog = new LinkedBlockingDeque<>();
            this.maximumMemorySize = maximumMemorySize;
            this.failFast = failFast;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
            if (this.closed.get()) {
                subscription.cancel();
            } else {
                subscription.request(1);
            }
        }

        @Override
        public void onNext(DataBuffer buffer) {
            if (this.closed.get()) {
                DataBufferUtils.release(buffer);
                return;
            }
            int readableByteCount = buffer.readableByteCount();
            int current = this.buffered.addAndGet(readableByteCount);
            if (current < this.maximumMemorySize) {
                this.subscription.request(1);
            }
            InputStream stream = buffer.asInputStream(true);
            this.backlog.add(new InputStreamWithSize(readableByteCount, stream));
            if (this.closed.get()) {
                DataBufferUtils.release(buffer);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (failFast) {
                Object next;
                while ((next = this.backlog.poll()) != null) {
                    if (next instanceof InputStreamWithSize) {
                        try {
                            ((InputStreamWithSize) next).inputStream.close();
                        } catch (Throwable t) {
                            throwable.addSuppressed(t);
                        }
                    }
                }
            }
            this.backlog.add(throwable);
        }

        @Override
        public void onComplete() {
            this.backlog.add(END);
        }

        private boolean forward() throws IOException {
            this.current.inputStream.close();
            try {
                Object next = this.backlog.take();
                if (next == END) {
                    this.current = null;
                    return true;
                } else if (next instanceof RuntimeException) {
                    close();
                    throw (RuntimeException) next;
                } else if (next instanceof IOException) {
                    close();
                    throw (IOException) next;
                } else if (next instanceof Throwable) {
                    close();
                    throw new IllegalStateException((Throwable) next);
                } else {
                    int buffer = buffered.addAndGet(-this.current.size);
                    if (buffer < this.maximumMemorySize) {
                        this.subscription.request(1);
                    }
                    this.current = (InputStreamWithSize) next;
                    return false;
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public int read() throws IOException {
            if (this.closed.get()) {
                throw new IOException("closed");
            } else if (this.current == null) {
                return -1;
            }
            int read;
            while ((read = this.current.inputStream.read()) == -1) {
                if (forward()) {
                    return -1;
                }
            }
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            Objects.checkFromIndexSize(off, len, b.length);
            if (this.closed.get()) {
                throw new IOException("closed");
            } else if (this.current == null) {
                return -1;
            }
            int sum = 0;
            do {
                int read = this.current.inputStream.read(b, off + sum, len - sum);
                if (read == -1) {
                    if (this.backlog.isEmpty()) {
                        return sum;
                    } else if (forward()) {
                        return sum == 0 ? -1 : sum;
                    }
                } else {
                    sum += read;
                }
            } while (sum < len);
            return sum;
        }

        @Override
        public int available() throws IOException {
            if (this.closed.get()) {
                throw new IOException("closed");
            } else if (this.current == null) {
                return 0;
            }
            int available = this.current.inputStream.available();
            for (Object value : this.backlog) {
                if (value instanceof InputStreamWithSize) {
                    available += ((InputStreamWithSize) value).inputStream.available();
                } else {
                    break;
                }
            }
            return available;
        }

        @Override
        public void close() throws IOException {
            if (this.closed.compareAndSet(false, true)) {
                if (this.subscription != null) {
                    this.subscription.cancel();
                }
                IOException exception = null;
                if (this.current != null) {
                    try {
                        this.current.inputStream.close();
                    } catch (IOException e) {
                        exception = e;
                    }
                }
                for (Object value : this.backlog) {
                    if (value instanceof InputStreamWithSize) {
                        try {
                            ((InputStreamWithSize) value).inputStream.close();
                        } catch (IOException e) {
                            if (exception == null) {
                                exception = e;
                            } else {
                                exception.addSuppressed(e);
                            }
                        }
                    }
                }
                if (exception != null) {
                    throw exception;
                }
            }
        }
    }

    static class InputStreamWithSize {

        final int size;

        final InputStream inputStream;

        InputStreamWithSize(int size, InputStream inputStream) {
            this.size = size;
            this.inputStream = inputStream;
        }
    }
}
