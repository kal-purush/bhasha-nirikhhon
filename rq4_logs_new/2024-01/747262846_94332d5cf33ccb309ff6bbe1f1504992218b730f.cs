using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NLayer.Core.DTOs;
using NLayer.Core.Models;
using NLayer.Core.Services;

namespace NLayer.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoryController : CustomController
    {
        private readonly IMapper _mapper;
        private readonly IService<Category> _service;

        public CategoryController(IMapper mapper, IService<Category> service)
        {
            _mapper = mapper;
            _service = service;
        }

        [HttpGet]
        public async Task<IActionResult> All()
        {
            var categories = await _service.GetAllAsync();
            var categoriesDTO = _mapper.Map<List<CategoryDTO>>(categories.ToList());

            return CreateActionResult(CustomResponseDTO<List<CategoryDTO>>.Success(200, categoriesDTO));

        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetById(int id)
        {
            var category = await _service.GetByIdAsync(id);
            var categoryDTO = _mapper.Map<CategoryDTO>(category);
            
            return CreateActionResult(CustomResponseDTO<CategoryDTO>.Success(200, categoryDTO));  
        }

        [HttpPost]
        public async Task<IActionResult> Save(CategoryDTO categoryDTO)
        {
            var category = await _service.AddAsync(_mapper.Map<Category>(categoryDTO));

            var _categoryDTO = _mapper.Map<CategoryDTO>(category);

            return CreateActionResult(CustomResponseDTO<NoContentDTO>.Success(204));
        }

        [HttpPut]
        public async Task<IActionResult> Update(CategoryDTO categoryDTO)
        {
            await _service.UpdateAsync(_mapper.Map<Category>(categoryDTO));

            return CreateActionResult(CustomResponseDTO<NoContentDTO>.Success(204));
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> Delete(int id)
        {
            var category = await _service.GetByIdAsync(id);

            await _service.RemoveAsync(category);

            return CreateActionResult(CustomResponseDTO<NoContentDTO>.Success(204));
        }
    }
}