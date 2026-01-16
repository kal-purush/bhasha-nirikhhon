using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BookLib.Models;
using BookLib.Services;
using Microsoft.AspNetCore.Mvc;


namespace BookLib.Controllers
{
    [Route("api/authors")] //指定该controller的路由地址
    [ApiController] //该属性是在core2.1中添加的特性，有些方便功能，比如自动模型验证，action参数来源推断等
    public class AuthorController : ControllerBase
    {
        public IAuthorRepository AuthorRepository { get; }

        public AuthorController(IAuthorRepository authorRepository)
        {
            AuthorRepository = authorRepository;
        }

        [HttpGet]
        public ActionResult<List<AuthorDto>> GetAuthors()
        {
            return AuthorRepository.GetAuthors().ToList();
        }

        [HttpGet("{authorId}", Name = nameof(GetAuthor))]//在[Route("api/authors")]这个router基础上传入authorId, 即api/authors/xxxxx
        public ActionResult<AuthorDto> GetAuthor(Guid authorId)
        {
            var author = AuthorRepository.GetAuthor(authorId);
            if (author == null)
            {
                return NotFound();
            }
            else
            {
                return author;
            }
        }

        [HttpPost]
        public IActionResult CreateAuthor(AuthorForCreationDto authorForCreationDto)
        {
            var authorDto = new AuthorDto
            {
                Name = authorForCreationDto.Name,
                Age = authorForCreationDto.Age,
                Email = authorForCreationDto.Email
            };

            AuthorRepository.AddAuthor(authorDto);

            //CreatedAtRoute方法是为了在新增authorDto之后返回对应的201状态码并且跳转到GetAuthor的路由
            //第一个参数就是GetAuthor的路由名称，第二个参数是GetAuthor的参数，这两个参数会组合成一个GetAuthor的Api并且在Reponse的Location里面返回
            //第三个参数是authorDto本身，会作为这个Api的结果返回
            return CreatedAtRoute(nameof(GetAuthor), new { authorId = authorDto.Id }, authorDto);
        }

        [HttpDelete("{authorId}")]
        public IActionResult DeleteAuthor(Guid authorId)
        {
            var author = AuthorRepository.GetAuthor(authorId);
            if(author == null)
            {
                return NotFound();
            }
            AuthorRepository.DeleteAuthor(author);
            //删除以后返回NoContent()，即204
            return NoContent();
        }

      
    }
}