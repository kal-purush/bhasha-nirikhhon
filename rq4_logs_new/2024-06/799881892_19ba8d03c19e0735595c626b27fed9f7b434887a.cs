using Application.UseCases.Products.Commands.GetProductsWithPagination;
using AutoMapper;
using System.ComponentModel.DataAnnotations;

namespace Application.Common.Models
{
    public class LookupDto
    {
        public int? DepartmentId { get; set; }
        public int? CategoryId { get; set; }
        public int? MinPrice { get; set; }

        public int? MaxPrice { get; set; }
        public string? Search { get; set; }
        public int PageNumber { get; set; } = 1;
        public int PageSize { get; set; } = 50;

        public bool HasFilterByDepartmentId()
        {
            return DepartmentId != null;
        }
        public bool HasFilterByCategoryId()
        {
            return CategoryId != null;
        }
        public bool HasFilterByPrice()
        {
            return MinPrice != null || MaxPrice != null;
        }
        public bool HasFilterBySearch()
        {
            return Search != null;
        }
        public bool HasFilterByCustomerReviewScore()
        {
            return false;
        }
        public bool HasFilter()
        {
            return HasFilterByDepartmentId()
                || HasFilterByCategoryId()
                || HasFilterByPrice()
                || HasFilterBySearch()
                || HasFilterByCustomerReviewScore();
        }

        public void ClearFilterByPrice()
        {
            MinPrice = null;
            MaxPrice = null;
        }

        public void ClearFilterByCustomerReviewScore()
        {
            //
        }

        private class Mapping : Profile
        {
            public Mapping()
            {
                CreateMap<GetProductsWithPaginationCommand, LookupDto>();
            }
        }
    }
}