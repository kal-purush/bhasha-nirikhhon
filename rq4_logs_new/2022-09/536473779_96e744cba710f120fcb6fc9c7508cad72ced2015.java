package com.kain.hap.proxy.admin.controller;

import org.springframework.http.HttpStatus;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kain.hap.proxy.tlv.Method;
import com.kain.hap.proxy.tlv.State;
import com.kain.hap.proxy.tlv.packet.MethodPacket;

import lombok.RequiredArgsConstructor;

/**
 *  internal endpoint to monitor session, steps, and so on. And start communication process.
 *  Should be disabled after end of development  
 * 
 *
 */

@RestController
@RequiredArgsConstructor
public class AdminToolController {
	
	private final MessageChannel outcome;

	
	@GetMapping("/hap/device/M1")
	@ResponseStatus(code = HttpStatus.OK)
	public void startDevicePacket() {
		outcome.send(MessageBuilder.withPayload(new MethodPacket(State.M1, Method.PAIR_SETUP)).build());
	}

}