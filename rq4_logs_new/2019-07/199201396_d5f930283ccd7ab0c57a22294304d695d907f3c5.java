package jhunions.isaiahgao.common;

import com.fasterxml.jackson.databind.JsonNode;

import io.javalin.Context;

public enum ScanResult {
	
	NO_SUCH_USER,
	CHECKED_OUT,
	CHECKED_IN,
	ERROR;
	
	private ScanResult() {
		this.json = "{\"scanresult\":\"" + this.toString() + "\"}";
	}
	
	private String json;
	
	public static ScanResult fromJson(String json) {
		JsonNode body;
		try {
			body = JsonUtils.getJson().readTree(json);
			return ScanResult.valueOf(body.get("scanresult").asText());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return ScanResult.ERROR;
	}
	
	public static ScanResult fromJson(Context ctx) {
		if (ctx.status() != 200) {
			return ScanResult.ERROR;
		}
		
		return fromJson(ctx.body());
	}
	
	public String toJson() {
		return this.json;
	}

}