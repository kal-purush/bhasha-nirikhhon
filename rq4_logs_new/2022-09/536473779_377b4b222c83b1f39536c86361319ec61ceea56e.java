package com.kain.hap.proxy.tlv.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.springframework.integration.ip.tcp.serializer.AbstractPooledBufferByteArraySerializer;
import org.springframework.integration.ip.tcp.serializer.SoftEndOfStreamException;

//TODO: rewrite with support HTTP protocol
public class PacketSerializer extends AbstractPooledBufferByteArraySerializer {

	private final String contentLengthHeader =  "Content-Length:"; 
	
	/**
	 * Reads the data in the inputStream to a byte[]. Data must be terminated
	 * by a single byte. Throws a {@link SoftEndOfStreamException} if the stream
	 * is closed immediately after the terminator (i.e. no data is in the process of
	 * being read).
	 */
	@Override
	protected byte[] doDeserialize(InputStream inputStream, byte[] buffer) throws IOException {
		int n = 0;
		int bite;
		int available = inputStream.available();
		int bodyLength = 0;
		logger.debug(() -> "Available to read: " + available);
		boolean duplicatedLf = false;
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
						inputStream.read(buffer, 0, bodyLength);
						n = bodyLength;
						break;
					}
					duplicatedLf = true;
					String header = new String(buffer);
					if (header.startsWith(contentLengthHeader)) {
						bodyLength = Integer.valueOf(header.substring(contentLengthHeader.length()).trim());		
					}
					// clear buffer
					Arrays.fill(buffer, (byte)0);
					n = 0;
					continue;
				} else {
					if (bite != '\r') {
						duplicatedLf = false;
					}
				}
				
				buffer[n++] = (byte) bite;
				int maxMessageSize = getMaxMessageSize();
				if (n >= maxMessageSize) {
					throw new IOException("Terminator CRLF not found before max message length: " + maxMessageSize);
				}
			}
			return copyToSizedArray(buffer, n);
		}
		catch (SoftEndOfStreamException e) { // NOSONAR catch and throw
			throw e; // it's an IO exception and we don't want an event for this
		}
		catch (IOException | RuntimeException ex) {
			publishEvent(ex, buffer, n);
			throw ex;
		}
	}

	/**
	 * Writes the byte[] to the stream and appends the terminator.
	 */
	@Override
	public void serialize(byte[] bytes, OutputStream outputStream) throws IOException {
		outputStream.write(bytes);
	}
}

