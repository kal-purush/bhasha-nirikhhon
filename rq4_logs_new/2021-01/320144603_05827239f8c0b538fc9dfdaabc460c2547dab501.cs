using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WordGame.API
{
	public class ApiResponse<TData> where TData : class
	{
		public TData? Data { get; protected set; }

		public string? Message { get; protected set; }

		public IEnumerable<ApiError> Errors { get; protected set; } = new List<ApiError>();

		public ApiResponse(TData data)
			=> Data = data;

		public ApiResponse(
			string? message,
			IEnumerable<ApiError>? errors = null)
		{
			Message = message;
			Errors = errors ?? new List<ApiError>();
		}

		public static implicit operator ApiResponse<TData>(TData data)
			=> new ApiResponse<TData>(data);

		public static implicit operator ApiResponse<TData>(ApiResponse response)
			=> new ApiResponse<TData>(response.Message, response.Errors);
	}

	public class ApiResponse : ApiResponse<object>
	{
		public ApiResponse(object data)
			: base(data)
		{ }

		public ApiResponse(string message, IEnumerable<ApiError>? errors = null)
			: base(message, errors)
		{ }
	}

	public class ApiError
	{
		public string Code { get; protected set; } = string.Empty;
		public string Message { get; protected set; } = string.Empty;
	}
}