using System.ComponentModel.DataAnnotations;

namespace AJS.Web.Models
{
    public class ChildCategoriesNavigationMenuPartialViewModel
    {
        [Required(ErrorMessage = ProjectConstants.ChildCategoryNavigationMenuPartialViewModelActionNameRequiredErrorMessage)]
        public string CategoryName { get; set; }

        [Required(ErrorMessage = ProjectConstants.ChildCategoryNavigationMenuPartialViewModelAreaNameRequiredErrorMessage)]
        public string Area { get; set; }

        [Required(ErrorMessage = ProjectConstants.ChildCategoryNavigationMenuPartialViewModelControllerNameRequiredErrorMessage)]
        public string Controller { get; set; }

        [Required(ErrorMessage = ProjectConstants.ChildCategoryNavigationMenuPartialViewModelActionNameRequiredErrorMessage)]
        public string Action { get; set; }

        [Required(ErrorMessage = ProjectConstants.ChildCategoryNavigationMenuPartialViewModelCategoryIdRequiredErrorMessage)]
        public string CategoryId { get; set; }
    }
}