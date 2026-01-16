using _1.DATA.Model;
using _1_API.ViewModel.GioHangChiTiet;
using _4.ClientView.StrConnection;
using _4.CusView.IServices;
using _4.CusView.ModelRequest;
using Microsoft.AspNetCore.Mvc;
using NToastNotify;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace _4.CusView.Controllers
{
    public class CartController : Controller
    {
        private readonly ILogger<CartController> _logger;
        private readonly IAllServices _services;
        private readonly IToastNotification _toast;
        public CartController(ILogger<CartController> logger, IAllServices services, IToastNotification toastNotification)
        {
            _logger = logger;
            _services = services;
            _toast = toastNotification;
        }
        public async Task<IActionResult> RemoveItem(Guid id)
        {
            await _services.Remove<GioHangChiTiet>(StrConnection.api + "GioHangChiTiets/GetById/", StrConnection.api + "GioHangChiTiets/Delete/", id);
            return RedirectToAction("Index");
        }
        public async Task<IActionResult> TangSL(Guid id)
        {
            try
            {
                string idgh = HttpContext.Session.GetString("idgh");

                var lstSizeSP = await _services.GetAll<SizeSanPham>(StrConnection.api + "SizeSanPhams/Get-All");
                var lstSanphamChitiet = await _services.GetAll<SanPhamChiTiet>(StrConnection.api + "SanphamChitiets/Get-All");
                var lstKichCo = await _services.GetAll<Size>(StrConnection.api + "Sizes/Get-All");
                var respons = await _services.GetAllViewCart<CartRequest>(StrConnection.api + "GioHangChiTiets/Get-All");
                var respon = respons.data.FirstOrDefault(x => x.idGiohang.ToString() == idgh && x.Id == id);

                var kc = lstKichCo.FirstOrDefault(x => x.Id == respon.IdSize);
                var sizesp = lstSizeSP.FirstOrDefault(x => x.IdSize == kc.Id);

                //var lstsl = (from a in lstSanphamChitiet
                //             join b in lstSizeSP on a.Id equals b.IdSanPhamChiTiet
                //             join c in lstKichCo.Where(x => x.Id == kc.Id) on b.IdSize equals c.Id
                //             join d in respons.data on c.Id equals d.IdSize
                //             select new { a, b, c, d }).FirstOrDefault();

                respon.SoLuong++;
                if (respon.SoLuong > sizesp.SoLuong)
                {
                    _toast.AddWarningToastMessage("Vượt quá số lượng trong kho!");
                }
                else
                {
                    await _services.Update(StrConnection.api + "GioHangChiTiets/Update/", respon, respon.Id);
                }
                return RedirectToAction("Index");
            }
            catch (Exception)
            {
                return RedirectToAction("Index");
            }

        }
        public async Task<IActionResult> GiamSL(Guid id)
        {
            try
            {
                string idgh = HttpContext.Session.GetString("idgh");
                var respons = await _services.GetAllViewCart<CartRequest>(StrConnection.api + "GioHangChiTiets/Get-All");
                var respon = respons.data.FirstOrDefault(x => x.idGiohang.ToString() == idgh && x.Id == id);
                --respon.SoLuong;
                if (respon.SoLuong <= 0)
                {
                    await _services.Remove<GioHangChiTiet>(StrConnection.api + "GioHangChiTiets/GetById/", StrConnection.api + "GioHangChiTiets/Delete/", respon.Id);
                    return RedirectToAction("Index");
                }
                await _services.Update(StrConnection.api + "GioHangChiTiets/Update/", respon, respon.Id);
                return RedirectToAction("Index");
            }
            catch (Exception)
            {
                return RedirectToAction("Index");
            }

        }
        public async Task<IActionResult> Index()
        {
            try
            {
                string idgh = HttpContext.Session.GetString("idgh");
                if (idgh == null)
                {
                    return RedirectToAction("DangNhap", "KhachHang");
                }
                else
                {
                    var result = await _services.GetAllViewCart<CartRequest>(StrConnection.api + "Giohangchitiets/get-all");
                    if (result != null)
                    {
                        foreach (var item in result.data)
                        {
                            result.numberOfItems = result.data.Where(x => x.idGiohang.ToString() == idgh).Sum(x => x.SoLuong);
                            result.totalPriceOfItems += (item.SoLuong * item.GiaBan);
                        }
                    }
                    return View(result);

                }
            }
            catch (Exception)
            {
                return RedirectToAction("Index", "Home");
                throw;
            }


        }
    }
}