using _1.DATA.Model;
using _4.ClientView.StrConnection;
using _4.CusView.IServices;
using _4.CusView.ModelRequest;
using Microsoft.AspNetCore.Mvc;

namespace _4.CusView.Helper
{
    public class HangViewComponent : ViewComponent
    {
        private readonly IAllServices _services;
        public HangViewComponent(IAllServices services)
        {
            _services = services;
        }
        public async Task<IViewComponentResult> InvokeAsync()
        {
            var lstTheLoai = await _services.GetAll<Hang>(StrConnection.api + "hangs/Get-All");
            return View(lstTheLoai);
        }
    }
}