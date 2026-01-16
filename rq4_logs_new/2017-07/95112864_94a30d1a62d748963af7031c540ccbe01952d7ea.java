package com.musala.sdcs.device.channel.type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.musala.sdcs.exception.InvalidCommandTypeException;
import com.musala.sdcs.util.Validator;

public class ColorType {
	private static final String INVALID_COLOR_MESSAGE = "Color must be hex value. Instead got $s";
	private static final Logger logger = LoggerFactory.getLogger(ColorType.class);
	private String val;

	public ColorType(String val) {
		setVal(val);
	}

	public String getVal() {
		return val;
	}

	public void setVal(String val) {
		if (Validator.validateColor(val)) {
			this.val = val;
		} else {
			String errorMessage = String.format(INVALID_COLOR_MESSAGE, val);

			logger.error(errorMessage);
			throw new InvalidCommandTypeException(errorMessage);
		}
	}
	
	@Override
	public String toString() {
		return getVal();
	}

}