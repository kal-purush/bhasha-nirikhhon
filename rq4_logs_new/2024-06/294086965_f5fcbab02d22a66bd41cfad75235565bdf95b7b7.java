package org.openmrs.module.epts.etl.conf.types;

public enum EtlProcessingModeType {
	
	SERIAL,
	PARALLEL;
	
	public boolean isSerial() {
		return this.equals(SERIAL);
	}
	
	public boolean isParallel() {
		return this.equals(PARALLEL);
	}
}