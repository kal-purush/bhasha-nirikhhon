package com.kain.hap.proxy.mdns;

import javax.jmdns.ServiceEvent;
import javax.jmdns.ServiceListener;

import lombok.extern.slf4j.Slf4j;


@Slf4j
//TODO: will be used as notifier 
public class MulticastDNSDiscoveryService implements ServiceListener {
    @Override
    public void serviceAdded(ServiceEvent event) {
        log.debug("Service added: {}", event.getInfo());
    }

    @Override
    public void serviceRemoved(ServiceEvent event) {
    	 log.debug("Service removed: {}", event.getInfo());
    }

    @Override
    public void serviceResolved(ServiceEvent event) {
    	 log.debug("Service resolved: {}", event.getInfo());
    }
}
	