using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using RealEstateApp.Data;
using RealEstateApp.Models;
using RealEstateApp.Services;

namespace RealEstateApp.Pages.EstateClick
{
    public class EstateOnClickModel : PageModel
    {
        public Estate CurrentEstate { get; set; }
        private readonly EstateService _estateService;


        public EstateOnClickModel(EstateService estateService)
        {
            _estateService = estateService;
        }
        public void OnGet(int id)
        {
            CurrentEstate = _estateService.GetById(id);
        }
    }
}