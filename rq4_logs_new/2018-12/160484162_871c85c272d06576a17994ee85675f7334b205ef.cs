using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using VirtoCommerce.Storefront.Infrastructure;
using VirtoCommerce.Storefront.Model;
using VirtoCommerce.Storefront.Model.Common;
using VirtoCommerce.Storefront.Model.CustomerReviews;

namespace VirtoCommerce.Storefront.Controllers.Api
{
    [StorefrontApiRoute("customerreview")]
    public class ApiCustomerReviewController : StorefrontControllerBase
    {

        private readonly ICustomerReviewService _customerReviewService;

        public ApiCustomerReviewController(IWorkContextAccessor workContextAccessor, IStorefrontUrlBuilder urlBuilder,
            ICustomerReviewService customerReviewService) : base(workContextAccessor, urlBuilder)
        {
            _customerReviewService = customerReviewService;
        }

        [HttpPost("search")]
        [AllowAnonymous]
        public async Task<ActionResult> Search([FromBody] CustomerReviewSearchCriteria criteria)
        {
            var result = await _customerReviewService.SearchReviewsAsync(criteria);
            return Json(result.ToList());
        }


    }
}