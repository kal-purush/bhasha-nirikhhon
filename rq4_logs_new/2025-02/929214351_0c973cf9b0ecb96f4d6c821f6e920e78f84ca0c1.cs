using AutoMapper;
using iPhoneBE.Data;
using iPhoneBE.Data.Model;
using iPhoneBE.Data.Models.CategoryModel;
using iPhoneBE.Data.ViewModels.CategoryDTO;
using iPhoneBE.Service.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;

namespace iPhoneBE.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoryController : ControllerBase
    {
        private readonly ICategoryServices _categoryServices;
        private readonly IMapper mapper;
        private readonly IUnitOfWork _unitOfWork;

        public CategoryController(ICategoryServices categoryServices, IMapper mapper, IUnitOfWork unitOfWork)
        {
            _categoryServices = categoryServices;
            this.mapper = mapper;
            _unitOfWork = unitOfWork;
        }

        [HttpGet]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<IEnumerable<CategoryViewModel>>> GetAll(string? categoryName = null)
        {
            var categories = await _categoryServices.GetAllAsync(categoryName);
            return Ok(mapper.Map<List<CategoryViewModel>>(categories));
        }

        [HttpGet("{id}")]
        //[Authorize(Roles = "Admin, Staff, Customer")]
        public async Task<ActionResult<Category>> GetById(int id)
        {
            var category = await _categoryServices.GetByIdAsync(id);
            if (category == null)
                return NotFound();

            return Ok(mapper.Map<CategoryViewModel>(category));
        }

        [HttpPost("create-category")]
        //[Authorize(Roles = "Admin, Staff")]
        public async Task<ActionResult<Category>> Add([FromBody] CreateCategoryModel Createcategory)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var category = mapper.Map<Category>(Createcategory);
                if (category == null)
                {
                    return BadRequest("Invalid category data.");
                }
                category = await _categoryServices.AddAsync(category);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<CategoryViewModel>(category));
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }



        [HttpPut("{id}")]
        //[Authorize(Roles = "Admin")]
        public async Task<IActionResult> Update(int id, [FromBody] UpdateCategoryModel updateCategory)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _unitOfWork.BeginTransaction();
            try
            {
                var existingCategory = await _categoryServices.GetByIdAsync(id);
                if (existingCategory == null)
                {
                    return NotFound($"Category with ID {id} not found.");
                }

                var category = mapper.Map<Category>(updateCategory);
                category = await _categoryServices.UpdateAsync(id, category);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<CategoryViewModel>(category));
            }
            catch (ArgumentNullException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NoContent();
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }

        [HttpDelete("{id}")]
        //[Authorize(Roles = "Admin")]
        public async Task<IActionResult> DeleteCategory(int id)
        {
            _unitOfWork.BeginTransaction();
            try
            {
                var existingCategory = await _categoryServices.GetByIdAsync(id);
                if (existingCategory == null)
                {
                    return NotFound($"Category with ID {id} not found.");
                }

                await _categoryServices.DeleteAsync(id);

                _unitOfWork.SaveChanges();
                _unitOfWork.CommitTransaction();

                return Ok(mapper.Map<CategoryViewModel>(existingCategory));
            }
            catch (KeyNotFoundException ex)
            {
                _unitOfWork.RollbackTransaction();
                return NotFound(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                _unitOfWork.RollbackTransaction();
                return StatusCode(StatusCodes.Status500InternalServerError, new { message = ex.Message });
            }
        }
    }
}