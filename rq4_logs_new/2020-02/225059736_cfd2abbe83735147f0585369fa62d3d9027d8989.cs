using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace AJS.Web.Models
{
    public class CategoriesNavigationMenuPartialViewModel
    {
        public string ParentCategoryId { get; set; }

        [Required(ErrorMessage = ProjectConstants.CategoryNavigationMenuPartialViewModelActionNameRequiredErrorMessage)]
        public string CategoryName { get; set; }

        [Required(ErrorMessage = ProjectConstants.CategoryNavigationMenuPartialViewModelAreaNameRequiredErrorMessage)]
        public string Area { get; set; }

        [Required(ErrorMessage = ProjectConstants.CategoryNavigationMenuPartialViewModelControllerNameRequiredErrorMessage)]
        public string Controller { get; set; }

        [Required(ErrorMessage = ProjectConstants.CategoryNavigationMenuPartialViewModelActionNameRequiredErrorMessage)]
        public string Action { get; set; }

        [Required(ErrorMessage = ProjectConstants.CategoryNavigationMenuPartialViewModelCategoryIdRequiredErrorMessage)]
        public string CategoryId { get; set; }

        public List<CategoriesNavigationMenuPartialViewModel> SubCategories { get; set; } = new List<CategoriesNavigationMenuPartialViewModel>();
    }
}