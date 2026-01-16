package com.kain.hap.proxy.tlv.integration;

import org.springframework.integration.handler.GenericHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import com.kain.hap.proxy.tlv.packet.BasePacket;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class PacketHandler implements GenericHandler<BasePacket>{

	@Override
	public Object handle(BasePacket payload, MessageHeaders headers) {
		log.debug("Here is packet: {}", payload);
		return payload;
	}

}