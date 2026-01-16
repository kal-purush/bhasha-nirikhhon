using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AutoMapper;
using BookLib.Entities;
using BookLib.Helpers;
using BookLib.Models;
using BookLib.Services;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace BookLib.Controllers
{
    [Route("api/authors")] //指定该controller的路由地址
    [ApiController] //该属性是在core2.1中添加的特性，有些方便功能，比如自动模型验证，action参数来源推断等
    public class AuthorController : ControllerBase
    {
        public IRepositoryWrapper RepositoryWrapper { get; }
        public IMapper Mapper { get; }

        public AuthorController(IRepositoryWrapper repositoryWrapper, IMapper mapper)
        {
            RepositoryWrapper = repositoryWrapper;
            Mapper = mapper;
        }

        [HttpGet(Name = nameof(GetAuthorsAsync))] // nameof(GetAuthorsAsync)一定不能漏，否则rl.Link(nameof(GetAuthorsAsync)会返回null
        public async Task<ActionResult<IEnumerable<AuthorDto>>> GetAuthorsAsync([FromQuery] AuthorResourceParameters parameters)//页码信息从url中取，因为不是resource数据
        {
            var pagedList = await RepositoryWrapper.Author.GetAllAsync(parameters);

            //匿名对象，包括所有分页的有关信息
            var paginationMetadata = new
            {
                //记录总数
                totalCount = pagedList.TotalCount,
                //每页记录数
                pageSize = pagedList.PageSize,
                //当前页
                currentPage = pagedList.CurrentPage,
                //所有页数
                totalPages = pagedList.TotalPages,
                //上一页的链接,Url.Link方法可以根据路由生成url
                previousePageLink = pagedList.HasPrevious ? Url.Link(nameof(GetAuthorsAsync), new
                {
                    pageNumber = pagedList.CurrentPage - 1,
                    pageSize = pagedList.PageSize
                }) : null,
                //下一页的链接
                nextPageLink = pagedList.HasNext ? Url.Link(nameof(GetAuthorsAsync), new
                {
                    pageNumber = pagedList.CurrentPage + 1,
                    pageSize = pagedList.PageSize
                }) : null
            };
            //将分页的元数据加入到响应的header内,自定义消息头"X-Pagination"
            Response.Headers.Add("X-Pagination", JsonConvert.SerializeObject(paginationMetadata));
            var authorDtoList = Mapper.Map<IEnumerable<AuthorDto>>(pagedList);
            return authorDtoList.ToList();
        }

        [HttpGet("{authorId}", Name = nameof(GetAuthorAsync))]//在[Route("api/authors")]这个router基础上传入authorId, 即api/authors/xxxxx
        public async Task<ActionResult<AuthorDto>> GetAuthorAsync(Guid authorId)
        {
            var author = await RepositoryWrapper.Author.GetByIdAsync(authorId);
            if (author == null)
            {
                return NotFound();
            }
            else
            {
                return Mapper.Map<AuthorDto>(author);
            }
        }

        [HttpPost]
        public async Task<IActionResult> CreateAuthorAsync(AuthorForCreationDto authorForCreationDto)
        {
            var author = Mapper.Map<Author>(authorForCreationDto);
            RepositoryWrapper.Author.Create(author);
            var result = await RepositoryWrapper.Author.SaveAsync();
            if (!result)
            {
                throw new Exception("Create resource author failed!");
            }
            var authorCreated = Mapper.Map<AuthorDto>(author);
            return CreatedAtRoute(nameof(CreateAuthorAsync), new { authorId = authorCreated.Id }, authorCreated);
        }

        [HttpDelete("{authorId}")]
        public async Task<IActionResult> DeleteAuthorAsync(Guid authorId)
        {
            var author = await RepositoryWrapper.Author.GetByIdAsync(authorId);
            if (author == null)
            {
                return NotFound();
            }
            RepositoryWrapper.Author.Delete(author);
            var result = await RepositoryWrapper.Author.SaveAsync();
            if (!result)
            {
                throw new Exception("Delete resource failed");
            }
            return NoContent();
        }


    }
}