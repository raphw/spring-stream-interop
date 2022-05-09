/*
 * Copyright 2002-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codes.rafael.springstreaminterop.webflux;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.AbstractClientHttpRequest;
import org.springframework.http.client.ClientHttpRequest;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestClientException;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * {@link ClientHttpRequest} implementation based on
 * Spring's {@link WebClient}.
 *
 * <p>Created via the {@link WebClientHttpRequestFactory}.
 */
final class WebClientHttpRequest extends AbstractClientHttpRequest {

	private final WebClient webClient;

	private final HttpMethod method;

	private final URI uri;

	private ByteArrayOutputStream bufferedOutput = new ByteArrayOutputStream(1024);

	WebClientHttpRequest(WebClient client, HttpMethod method, URI uri) {
		this.webClient = client;
		this.method = method;
		this.uri = uri;
	}


	@Override
	public HttpMethod getMethod() {
		return this.method;
	}

	@Override
	@Deprecated
	public String getMethodValue() {
		return this.method.name();
	}

	@Override
	public URI getURI() {
		return this.uri;
	}

	@Override
	protected OutputStream getBodyInternal(HttpHeaders headers) throws IOException {
		return this.bufferedOutput;
	}

	@Override
	protected ClientHttpResponse executeInternal(HttpHeaders headers) throws IOException {
		byte[] bytes = this.bufferedOutput.toByteArray();
		if (headers.getContentLength() < 0) {
			headers.setContentLength(bytes.length);
		}
		ClientHttpResponse result = executeInternal(headers, bytes);
		this.bufferedOutput = new ByteArrayOutputStream(0);
		return result;
	}

	private ClientHttpResponse executeInternal(HttpHeaders headers, byte[] bufferedOutput) {
		WebClient.RequestHeadersSpec<?> request;
		if (bufferedOutput.length > 0) {
			request = this.webClient.method(this.method).uri(this.uri).bodyValue(bufferedOutput);
		} else {
			request = this.webClient.method(this.method).uri(this.uri);
		}

		request.headers(value -> value.addAll(headers));

		@SuppressWarnings("deprecation")
		Mono<ClientResponse> response = request.exchange();
		try {
		return new WebClientHttpResponse(response.block());
		} catch (RestClientException e) {
			e.printStackTrace();
			return null;
		}
	}
}
