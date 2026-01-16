package com.kain.hap.proxy.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Transformers;
import org.springframework.integration.ip.dsl.Tcp;

import com.kain.hap.proxy.tlv.serialize.TlvDeserializer;

@Configuration
public class ServerConfig {
	
	@Bean
	public IntegrationFlow server() {
	    return IntegrationFlows.from(Tcp.inboundAdapter(Tcp.netServer(12345)
	                            .deserializer(new TlvDeserializer())
	                            .backlog(30))
	                        .errorChannel("tcpIn.errorChannel")
	                        .id("tcpIn"))
	            .transform(Transformers.objectToString())
	            .channel("tcpInbound")
	            .get();
	}
}