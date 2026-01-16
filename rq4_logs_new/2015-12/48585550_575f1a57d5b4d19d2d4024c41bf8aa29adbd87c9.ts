'use strict';

import { jBinary } from 'jBinary';

export class EDFData{
	header: EDFHeader;
	signal: EDFSignal;
	annotations: EDFAnnotation[];
}

export class EDFHeader{
	idCode: string;
	subjectID: string;
	recordingID: string;
	startDate: string;
	startTime: string;
	bytesInHeader: number = 0;
	formatVersion: string;
	numberOfRecords: number = 0;
	durationOfRecords: number = 0.0;
	numberOfChannels: number = 0;
	channelLabels: string[];
	transducerTypes: string[];
	dimensions: string[];
	minInUnits: string[];
	maxInUnits: string[];
	digitalMin: number[];
	digitalMax: number[];
	prefilterings: string[];
	numberOfSamples: number[];
	reserved: Buffer;
}

export class EDFSignal{
	unitsInDigit: number[];
	digitalValues: number[][];
	valuesInUnits: number[][];
}

export class EDFAnnotation{
	onSet: number = 0.0;
	duration: number = 0.0;
	annotations: string[];

	constructor(onSet: string, duration: string, annotations: string[]){
		this.onSet = parseFloat(onSet);

		if(!!duration){
			this.duration = parseFloat(duration);
		}

		for(let i = 0; i < annotations.length; i++){
			if(!!annotations[i]) {
				this.annotations.push(annotations[i]);
			}
		}
	}

	public toString = () => {
		return "Annotation [onSet=" + this.onSet + ", duration=" + this.duration + ", annotations=" + this.annotations + "]";
	}
}

export class EDFConstants{
	public static IDENTIFICATION_CODE_SIZE = 8;
	public static LOCAL_SUBJECT_IDENTIFICATION_SIZE = 80;
	public static LOCAL_RECORDING_IDENTIFICATION_SIZE = 80;
	public static START_DATE_SIZE = 8;
	public static START_TIME_SIZE = 8;
	public static HEADER_SIZE = 8;
	public static DATA_FORMAT_VERSION_SIZE = 44;
	public static DURATION_DATA_RECORDS_SIZE = 8;
	public static NUMBER_OF_DATA_RECORDS_SIZE = 8;
	public static NUMBER_OF_CHANNELS_SIZE = 4;

	public static LABEL_OF_CHANNEL_SIZE = 16;
	public static TRANSDUCER_TYPE_SIZE = 80;
	public static PHYSICAL_DIMENSION_OF_CHANNEL_SIZE = 8;
	public static PHYSICAL_MIN_IN_UNITS_SIZE = 8;
	public static PHYSICAL_MAX_IN_UNITS_SIZE = 8;
	public static DIGITAL_MIN_SIZE = 8;
	public static DIGITAL_MAX_SIZE = 8;
	public static PREFILTERING_SIZE = 80;
	public static NUMBER_OF_SAMPLES_SIZE = 8;
	public static RESERVED_SIZE = 32;

	/** The size of the EDF-Header-Record containing information about the recording */
	public static HEADER_SIZE_RECORDING_INFO
		= EDFConstants.IDENTIFICATION_CODE_SIZE + EDFConstants.LOCAL_SUBJECT_IDENTIFICATION_SIZE + EDFConstants.LOCAL_REOCRDING_IDENTIFICATION_SIZE
			+ EDFConstants.START_DATE_SIZE + EDFConstants.START_TIME_SIZE + EDFConstants.HEADER_SIZE + EDFConstants.DATA_FORMAT_VERSION_SIZE
			+ EDFConstants.DURATION_DATA_RECORDS_SIZE + EDFConstants.NUMBER_OF_DATA_RECORDS_SIZE + EDFConstants.NUMBER_OF_CHANELS_SIZE;

	/** The size per channel of the EDF-Header-Record containing information a channel of the recording */
	public static HEADER_SIZE_PER_CHANNEL
		= EDFConstants.LABEL_OF_CHANNEL_SIZE + EDFConstants.TRANSDUCER_TYPE_SIZE + EDFConstants.PHYSICAL_DIMENSION_OF_CHANNEL_SIZE
			+ EDFConstants.PHYSICAL_MIN_IN_UNITS_SIZE + EDFConstants.PHYSICAL_MAX_IN_UNITS_SIZE + EDFConstants.DIGITAL_MIN_SIZE
			+ EDFConstants.DIGITAL_MAX_SIZE	+ EDFConstants.PREFILTERING_SIZE + EDFConstants.NUMBER_OF_SAMPLES_SIZE + EDFConstants.RESERVED_SIZE;
}