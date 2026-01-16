package com.kain.hap.proxy.tlv.serialize;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.springframework.core.serializer.Serializer;

import com.google.common.primitives.Bytes;
import com.kain.hap.proxy.tlv.packet.BasePacket;

public class BasePacketSerializer implements Serializer<BasePacket>{
	private static final TlvMapper MAPPER = TlvMapper.INSTANCE;
	private static final byte[] CRLF = "\r\n".getBytes();
	private static final byte[] COLON = ":".getBytes();
	private static final byte[] RESPONSE_CODE = "HTTP/1.1 200 OK".getBytes();
	private static final byte[] LENGTH_HEADER = "Content-Length".getBytes();

	@Override
	public void serialize(BasePacket packet, OutputStream outputStream) throws IOException {
		byte[] body = MAPPER.writeValue(packet);
		byte[] headers = Map.of("Content-Type", "application/pairing+tlv8")
				.entrySet()
				.stream()
				.map(e -> Bytes.concat(e.getKey().getBytes(), COLON, e.getValue().getBytes(), CRLF))
				.reduce(new byte[0], Bytes::concat);


		byte[] raw = Bytes.concat(RESPONSE_CODE, CRLF);
		raw = Bytes.concat(raw, headers,  LENGTH_HEADER, COLON, String.valueOf(body.length).getBytes(), CRLF);
		raw = Bytes.concat(raw, CRLF, body);

		outputStream.write(raw);
	}

}