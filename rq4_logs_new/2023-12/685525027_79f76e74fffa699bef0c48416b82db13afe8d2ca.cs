﻿using Chirp.Core;
using FluentValidation;
using Microsoft.AspNetCore.Http.Extensions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;

namespace Chirp.Web.Pages;

public class TrendingModel : PageModel
{
	private readonly IAuthorRepository AuthorRepository;
	private readonly ICheepRepository CheepRepository;
	private readonly IValidator<CreateCheepDTO> CheepValidator;
	public required List<CheepDTO> Cheeps { get; set; }

	public required List<AuthorDTO> Following { get; set; }

	[BindProperty]
	public string? CheepMessage { get; set; }

	public bool InvalidCheep { get; set; }
	public string? ErrorMessage { get; set; }
	public int PageNumber { get; set; }
	public int LastPageNumber { get; set; }
	public string? PageUrl { get; set; }

	public TrendingModel(IAuthorRepository authorRepository, ICheepRepository cheepRepository, IValidator<CreateCheepDTO> _cheepValidator)
	{
		AuthorRepository = authorRepository;
		CheepRepository = cheepRepository;
		CheepValidator = _cheepValidator;
	}

	public ActionResult OnGet([FromQuery(Name = "page")] int page = 1)
	{
		if (User.Identity != null && User.Identity.IsAuthenticated)
		{
			string name = (User.Identity?.Name) ?? throw new Exception("Name is null!");
			Following = AuthorRepository.GetFollowing(name);
		}

		Cheeps = CheepRepository.GetMostLikedCheeps(page);
		PageNumber = page;
		LastPageNumber = CheepRepository.GetPageAmount();
		PageUrl = HttpContext.Request.GetEncodedUrl().Split("?")[0];

		return Page();
	}
	public async Task<IActionResult> OnPost()
	{
		InvalidCheep = false;
		try
		{
			if (CheepMessage == null)
			{
				throw new Exception("Cheep is empty!");
			}

			string name = (User.Identity?.Name) ?? throw new Exception("Error in getting username");
			AuthorDTO? user = AuthorRepository.GetAuthorByName(name);

			if (user == null)
			{
				string token = User.FindFirst("idp_access_token")?.Value
					?? throw new Exception("Github token not found");

				string email = await GithubHelper.GetUserEmailGithub(token, name);

				AuthorRepository.CreateNewAuthor(name, email);
				user = AuthorRepository.GetAuthorByName(name) ?? throw new Exception("Error when getting user with name: " + name);
			}

			CreateCheepDTO cheep = new CreateCheepDTO()
			{
				Text = CheepMessage,
				Name = user.Name,
				Email = user.Email
			};

			CheepValidator.ValidateAndThrow(cheep);

			CheepRepository.CreateNewCheep(cheep);
			CheepMessage = null;
		}
		catch (Exception e)
		{
			ErrorMessage = e.Message;
			InvalidCheep = true;
		}

		return OnGet();
	}

	public IActionResult OnPostLike(Guid cheep)
	{
		if (User.Identity == null || !User.Identity.IsAuthenticated || User.Identity.Name == null)
		{
			return Unauthorized();
		}

		try
		{
			CheepRepository.LikeCheep(cheep, User.Identity.Name);
		}
		catch (Exception e)
		{
			ErrorMessage = e.Message;
		}

		return OnGet();
	}

	public IActionResult OnPostUnlike(Guid cheep)
	{
		if (User.Identity == null || !User.Identity.IsAuthenticated || User.Identity.Name == null)
		{
			return Unauthorized();
		}

		try
		{
			CheepRepository.UnlikeCheep(cheep, User.Identity.Name);
		}
		catch (Exception e)
		{
			ErrorMessage = e.Message;
		}

		return OnGet();
	}

	public async Task<IActionResult> OnPostFollow(string followeeName, string followerName)
	{
		string name = (User.Identity?.Name) ?? throw new Exception("Name is null!");

		if (AuthorRepository.GetAuthorByName(name) == null)
		{
			string token = User.FindFirst("idp_access_token")?.Value
					?? throw new Exception("Github token not found");
			string email = await GithubHelper.GetUserEmailGithub(token, name);

			AuthorRepository.CreateNewAuthor(name, email);
		}

		AuthorRepository.FollowAuthor(followerName ?? throw new Exception("Name is null!"), followeeName);
		return Redirect("/");
	}

	public IActionResult OnPostUnfollow(string followeeName, string followerName)
	{
		AuthorRepository.UnfollowAuthor(followerName ?? throw new Exception("Name is null!"), followeeName);
		return Redirect(PageUrl ?? "/");
	}
}