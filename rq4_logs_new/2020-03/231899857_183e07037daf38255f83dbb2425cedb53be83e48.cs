using ApiResponse.ActionResults.NotificationResult;
using ApiResponse.ActionResults.ZipResult;

using Microsoft.AspNetCore.Mvc;

using System.Collections.Generic;

namespace ApiResponse.ActionResults
{
	public static class ControllerExtensions
	{
		public static NotificationActionResultBuilder<T> Notify<T>(this ControllerBase controller, T response)
		{
			return new NotificationActionResultBuilder<T>(response);
		}

		public static ZipActionResult Zip(this ControllerBase controller, IEnumerable<FileItem> response, string fileName = "Photos")
		{
			byte[] archive = new Zipper().Zip(response).ToArray();

			return new ZipActionResult(archive, fileName);
		}
	}
}