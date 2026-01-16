package com.kain.hap.proxy.tlv.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.serializer.Deserializer;
import org.springframework.integration.ip.tcp.serializer.SoftEndOfStreamException;
import org.springframework.integration.ip.tcp.serializer.TcpDeserializationExceptionEvent;

import com.kain.hap.proxy.tlv.packet.HapRequest;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpPacketSerializer implements Deserializer<HapRequest>, ApplicationEventPublisherAware {

	private final String LENGTH_HEADER = "Content-Length:";
	private final String METHOD_SUFIX = "HTTP/1.1";

	private final int DEFAULT_MAX_MESSAGE_SIZE = 2048;

	private ApplicationEventPublisher applicationEventPublisher;
	private TlvMapper mapper = TlvMapper.INSTANCE;

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public HapRequest deserialize(InputStream inputStream) throws IOException {
		// TODO: check max size of request;
		byte[] buffer = new byte[DEFAULT_MAX_MESSAGE_SIZE];
		int n = 0;
		int bite;
		int available = inputStream.available();
		int bodyLength = 0;
		log.debug("Available to read: {}", available);
		boolean duplicatedLf = false;

		HapRequest request = new HapRequest();

		try {
			while (true) {
				bite = inputStream.read();
				if (bite < 0 && n == 0) {
					throw new SoftEndOfStreamException("Stream closed between payloads");
				}
				checkClosure(bite);
				if (n > 0 && bite == '\n' && buffer[n - 1] == '\r') {
					if (duplicatedLf) {
						// start read body
						byte[] body = new byte[bodyLength];
						inputStream.read(body, 0, bodyLength);
						request.setBody(mapper.readPacket(body));
						break;
					}
					duplicatedLf = true;
					String header = new String(buffer).trim();
					if (header.startsWith(LENGTH_HEADER)) {
						bodyLength = Integer.valueOf(header.substring(LENGTH_HEADER.length()).trim());
					}
					// get request method and endpoint
					if (header.endsWith(METHOD_SUFIX)) {
						String[] splited = header.split(" ", 2);
						request.setMethod(splited[0]);
						request.setEndpoint(splited[1]);
					} else {
						request.addHeader(header);
					}

					Arrays.fill(buffer, (byte) 0);
					n = 0;
					continue;
				} else {
					if (bite != '\r') {
						duplicatedLf = false;
					}
				}

				buffer[n++] = (byte) bite;
				if (n >= DEFAULT_MAX_MESSAGE_SIZE) {
					throw new IOException(
							"Terminator CRLF not found before max message length: " + DEFAULT_MAX_MESSAGE_SIZE);
				}
			}
		} catch (SoftEndOfStreamException e) { // NOSONAR catch and throw
			throw e; // it's an IO exception and we don't want an event for this
		} catch (IOException | RuntimeException ex) {
			publishEvent(ex, buffer, n);
			throw ex;
		}
		return request;
	}

	protected void checkClosure(int bite) throws IOException {
		if (bite < 0) {
			log.debug("Socket closed during message assembly");
			throw new IOException("Socket closed during message assembly");
		}
	}

	protected void publishEvent(Exception cause, byte[] buffer, int offset) {
		TcpDeserializationExceptionEvent event = new TcpDeserializationExceptionEvent(this, cause, buffer, offset);
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		} else {
			log.trace("No event publisher for {}", event);
		}
	}

}