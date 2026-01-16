using Circle.Web.Models.Post;
using Microsoft.AspNetCore.Mvc;
using System.Net.Mail;

namespace Circle.Web.Controllers
{
	public class PostController : Controller
	{
		[HttpGet]
		public IActionResult Create()
		{
			return View();
		}

		[HttpPost]
		public async Task<IActionResult> CreateConfirm(CreatePostModel createPostModel)
		{
			//TODO
			return Redirect("/");
		}
		
		//private async Task<string> UploadPhoto(IFormFile photo)
		//{
		//	var uploadResponse = await this.cloudinaryService.UploadFile(photo);
		//
		//	if (uploadResponse == null)
		//	{
		//		return null;
		//	}
		//
		//	return uploadResponse["url"].ToString();
		//}
	}
}