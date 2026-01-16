using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using NLayer.Core.DTOs;
using NLayer.Core.Models;
using NLayer.Core.Services;

namespace NLayer.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BookController : CustomController
    {
        private readonly IMapper _mapper;
        private readonly IService<Book> _service;

        public BookController(IMapper mapper, IService<Book> service)
        {
            _mapper = mapper;
            _service = service;
        }

        [HttpGet]
        public async Task<IActionResult> All()
        {
            var books = await _service.GetAllAsync();
            var booksDTO = _mapper.Map<List<BookDTO>>(books.ToList());
            return CreateActionResult(CustomResponseDTO<List<BookDTO>>.Success(200, booksDTO));
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var book = await _service.GetByIdAsync(id);
            var bookDTO = _mapper.Map<BookDTO>(book);
            return CreateActionResult(CustomResponseDTO<BookDTO>.Success(200, bookDTO));

        }

        [HttpPost]
        public async Task<IActionResult> Save(BookDTO bookDTO)
        {
            var book = await _service.AddAsync(_mapper.Map<Book>(bookDTO));

            var _bookDTO = _mapper.Map<BookDTO>(book);

            return CreateActionResult(CustomResponseDTO<BookDTO>.Success(201, _bookDTO));
        }

        [HttpPut]
        public async Task<IActionResult> Update(BookDTO bookDTO)
        {
            await _service.UpdateAsync(_mapper.Map<Book>(bookDTO));

            return CreateActionResult(CustomResponseDTO<NoContentDTO>.Success(204));
        }

        [HttpDelete]
        public async Task<IActionResult> Remove(int id)
        {
            var book = await _service.GetByIdAsync(id);

            await _service.RemoveAsync(book);

            var bookDTO = _mapper.Map<BookDTO>(book);

            return CreateActionResult(CustomResponseDTO<NoContentDTO>.Success(204));
        }
    }
}