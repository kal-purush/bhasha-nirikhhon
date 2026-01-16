using System.Collections.Generic;
using System.Threading.Tasks;
using Api.Dtos;
using AutoMapper;
using Core.Entities.Halko;
using Core.Interfaces;
using Core.Specifications;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [Authorize]
    public class TransactionController : BaseApiController
    {
        private readonly IUnitOfWork _unitOfWork;
        private readonly IMapper _mapper;

        public TransactionController(
            IUnitOfWork unitOfWork,
            IMapper mapper)
        {
            _unitOfWork = unitOfWork;
            _mapper = mapper;
        }


        [HttpGet ( "product-categories" )]
        public async Task<ActionResult<List<ProductCategoriesToReturnDto>>> GetProductCategoriesAsync()
        {
            var productCategories = await _unitOfWork.Repository<ProductCategory>()
                .ListAsync ( new ProductCategoriesSpecification() );

            var productCategoriesToReturn = _mapper.Map<IReadOnlyList<ProductCategoriesToReturnDto>> ( productCategories );

            return Ok ( productCategoriesToReturn );
        }
        
    }
}