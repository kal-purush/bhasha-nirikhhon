package com.dyspersja.mp3metadataeditor.metadata;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ID3v2FrameTag {
	TITLE("TIT2"),
	SONG_ARTIST("TPE1"),
	ALBUM_COVER("APIC"),
	TRACK("TRCK"),
	PART_OF_SET("TPOS"),
	ALBUM("TALB"),
	ALBUM_ARTIST("TPE2"),
	GENRE("TCON"),
	YEAR("TYER"),

	UNKNOWN("UNKNOWN");

	private final String tag;

	public static ID3v2FrameTag fromString(String tag) {
		for (ID3v2FrameTag frameTag : ID3v2FrameTag.values()) {
			if (frameTag.getTag().equals(tag)) {
				return frameTag;
			}
		}
		return UNKNOWN;
	}
}