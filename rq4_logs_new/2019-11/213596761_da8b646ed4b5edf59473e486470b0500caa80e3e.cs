using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using servicer.API.Data;
using servicer.API.Dtos;
using servicer.API.Helpers;
using servicer.API.Models;

namespace servicer.API.Controllers
{
    [ServiceFilter(typeof(SetLastActive))]
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class ProductSpecificationsController : ControllerBase
    {
        private readonly IServicerRepository _repository;
        private readonly IMapper _mapper;
        public ProductSpecificationsController(IServicerRepository repository, IMapper mapper)
        {
            _repository = repository;
            _mapper = mapper;
        }

        [HttpGet]
        public async Task<IActionResult> GetProductSpecifications()
        {
            var productSpecifications = await _repository.GetProductSpecifications();
            var productSpecificationsToReturn = _mapper.Map<IEnumerable<ProductSpecificationForListDto>>(productSpecifications);

            return Ok(productSpecificationsToReturn);
        }

        [HttpGet("{id}", Name = "GetProductSpecification")]
        public async Task<IActionResult> GetProductSpecification(int id)
        {
            var productSpecification = await _repository.GetProductSpecification(id);
            var productSpecificationToReturn = _mapper.Map<ProductSpecificationForDetailDto>(productSpecification);

            return Ok(productSpecificationToReturn);
        }

        [HttpPost]
        public async Task<IActionResult> CreateProductSpecification(ProductSpecificationForSendDto productSpecificationForSendDto)
        {
            var productSpecificationToCreate = _mapper.Map<ProductSpecification>(productSpecificationForSendDto);

            var createdProductSpecification = await _repository.CreateProductSpecification(productSpecificationToCreate);
            var productSpecificationsToReturn = _mapper.Map<ProductSpecificationForDetailDto>(createdProductSpecification);

            return CreatedAtRoute("GetProductSpecification", new { controller = "ProductSpecifications", id = createdProductSpecification.Id }, productSpecificationsToReturn);
        }

        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateProductSpecification(int id, ProductSpecificationForUpdateDto productSpecificationForUpdateDto)
        {
            if (UserRole.Admin.ToString() != User.FindFirst(ClaimTypes.Role).Value.ToString())
            {
                return Unauthorized();
            }

            var productSpecificationFromRepo = await _repository.GetProductSpecification(id);
            _mapper.Map(productSpecificationForUpdateDto, productSpecificationFromRepo);

            if (await _repository.SaveAll())
                return NoContent();

            throw new Exception($"Błąd przy edytowaniu produktu o id: {id}.");
        }

        [HttpPut("{id}/changeisactive")]
        public async Task<IActionResult> ChangeIsActive(int id)
        {
            if (UserRole.Admin.ToString() != User.FindFirst(ClaimTypes.Role).Value.ToString())
            {
                return Unauthorized();
            }

            var productSpecificationToChangeIsActive = await _repository.GetProductSpecification(id);
            productSpecificationToChangeIsActive.IsActive = !productSpecificationToChangeIsActive.IsActive;

            await _repository.SaveAll();

            return Ok(new
            {
                message = "Zmieniono flagę aktywności."
            });
        }
    }
}