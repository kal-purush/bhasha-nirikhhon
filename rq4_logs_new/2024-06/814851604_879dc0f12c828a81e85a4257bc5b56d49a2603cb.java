package com.bookshelf.domain.enums;

public enum Format {
	HARDCOVER(0, "FORMAT_HARDCOVER"), PAPERBACK(1, "FORMAT_PAPERBACK"), EBOOK(2, "FORMAT_EBOOK"), AUDIOBOOK(2, "FORMAT_AUDIOBOOK");

	private Integer code;
	private String description;
	
	private Format(Integer code, String description) {
		this.code = code;
		this.description = description;
	}

	public Integer getCode() {
		return code;
	}

	public String getDescription() {
		return description;
	}
	
	public static Format toEnum(Integer code) {
		if ( code == null ) {
			return null;
		}
		
		for ( Format value : Format.values()) {
			if (code.equals(value.getCode())) {
				return value;
			}
		}
		
		throw new IllegalArgumentException("Invalid format.");
	}
}