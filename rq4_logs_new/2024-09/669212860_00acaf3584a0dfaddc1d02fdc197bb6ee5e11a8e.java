package com.ubiqube.etsi.mano.config.properties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ManoConfig {
	/**
	 * Type of installation, or given name. This value is used for Queues and topics
	 * name.
	 */
	private String type;
}