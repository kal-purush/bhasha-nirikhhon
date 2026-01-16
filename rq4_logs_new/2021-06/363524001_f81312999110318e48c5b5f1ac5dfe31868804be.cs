using System;
using System.Threading.Tasks;
using Application.Articles;
using Domain;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [AllowAnonymous]
    public class ArticlesController : BaseApiController
    {
        [HttpGet]
        public async Task<IActionResult> GetArticles()
        {
            return HandleResult(await Mediator.Send(new List.Query()));
        }
       

        [HttpGet("{id}")]
        public async Task<IActionResult> GetArticle(Guid id)
        {
            return HandleResult(await Mediator.Send(new Details.Query{Id = id}));
        }

        [HttpPost]
        public async Task<IActionResult> CreateArticle(Article article)
        {
            return HandleResult(await Mediator.Send(new Create.Command {Article = article}));
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> EditArticle(Guid id, Article article)
        {
            article.Id = id;
            return HandleResult(await Mediator.Send(new Edit.Command{Article = article}));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteArticle(Guid id)
        {
            return HandleResult(await Mediator.Send(new Delete.Command{Id = id}));
        }
    }
}