package com.project.quanlycanghangkhong.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(name = "ApiErrorResponse", description = "Response cho các trường hợp lỗi", example = "{\"message\": \"Không tìm thấy tài nguyên\", \"statusCode\": 404, \"errorDetails\": null, \"success\": false}")
public class ApiErrorResponse {
	@Schema(description = "Thông báo lỗi", example = "Không tìm thấy tài nguyên")
	private String message;

	@Schema(description = "Mã trạng thái HTTP", example = "404")
	private int statusCode;

	@Schema(description = "Chi tiết lỗi (nếu có)", nullable = true, example = "null")
	private Object errorDetails;

	@Schema(description = "Trạng thái thành công hay thất bại", example = "false")
	private boolean success;

	// Getter, Setter, Constructor
	public ApiErrorResponse() {
	}

	public ApiErrorResponse(String message, int statusCode, Object errorDetails, boolean success) {
		this.message = message;
		this.statusCode = statusCode;
		this.errorDetails = errorDetails;
		this.success = success;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public Object getErrorDetails() {
		return errorDetails;
	}

	public void setErrorDetails(Object errorDetails) {
		this.errorDetails = errorDetails;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}
}