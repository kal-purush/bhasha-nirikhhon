using FirstWebAPI.Entities;
using FirstWebAPI.Heplers;
using FirstWebAPI.Models;
using FirstWebAPI.Payload.Response;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FirstWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CategoryController : ControllerBase
    {
        private readonly DbContextHelper _context;
        public CategoryController(DbContextHelper context)
        {
            _context = context;
        }

        [HttpGet]
        public IActionResult GetAll()
        {
            var categories = _context.Categories.ToList();

            return Ok(
                new BaseResponse { StatusCode = 200, Data = categories}
                );
        }

        [HttpGet("{Id}")]
        public IActionResult GetById(int Id)
        {
            var categories = _context.Categories.SingleOrDefault(
                c => c.CategoryId == Id
                );
            if(categories == null)
            {
                return NotFound(
                    new BaseResponse { StatusCode = 404, Message = "Not Found Category with Id " + Id}
                    );
            }
            return Ok(
                new BaseResponse { StatusCode = 200, Data = categories}
                );
        }

        [HttpPost]
        public IActionResult Create(Category category)
        {
            try
            {
                CategoryEntity newCategory = new CategoryEntity()
                {
                    Name = category.Name
                };
                _context.Add(newCategory);
                _context.SaveChanges();
                return Ok(
                    new BaseResponse { StatusCode = 201, Message = "Create Sucess"}
                    );
            }
            catch
            {
                return BadRequest();
            }
        }

        [HttpPut]
        public IActionResult Update(int CategoryId, Category category)
        {
            try
            {
                var oldCategory 
                    = _context.Categories.Where(x => x.CategoryId == CategoryId ).FirstOrDefault();

                if(oldCategory == null)
                {
                    return NotFound();
                }

                oldCategory.Name = category.Name;
                _context.SaveChanges();
                return Ok(
                    new BaseResponse { StatusCode = 200, Message = "Update Sucess"}
                    );
            }
            catch
            {
                return BadRequest();
            }
        }
    }
}