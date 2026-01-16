using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ELibary.Data;
using ELibary.Models;

namespace ELibary.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BaiGiang_TaiNguyenController : ControllerBase
    {
        private readonly ELibaryContext _context;

        public BaiGiang_TaiNguyenController(ELibaryContext context)
        {
            _context = context;
        }

        [Route("api/TaiNguyens")]
        [HttpGet]
        public IActionResult GetTaiNguyen(string mon = null, string ten = null)
        {
            if (mon == null && ten == null)
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                           where c.PhanLoai == false
                           select new
                           {
                               c.Id,
                               c.LoaiFile,
                               c.Ten,
                               c.TenMon,
                               c.NguoiChinhSua,
                               c.NgayChinhSuaCuoi,
                               c.KichThuoc
                           });
                return Ok(get);
            }
            else if (mon != null)
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                           where c.MaMon == mon && c.PhanLoai == false
                           select new
                           {
                               c.Id,
                               c.LoaiFile,
                               c.Ten,
                               c.TenMon,
                               c.NguoiChinhSua,
                               c.NgayChinhSuaCuoi,
                               c.KichThuoc
                           }).OrderBy(x => x.Ten);
                return Ok(get);
            }
            else
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                       where c.Ten.Contains(ten) && c.PhanLoai == false
                       select new
                       {
                           c.Id,
                           c.LoaiFile,
                           c.Ten,
                           c.TenMon,
                           c.NguoiChinhSua,
                           c.NgayChinhSuaCuoi,
                           c.KichThuoc
                       }).OrderBy(x => x.Ten);
                return Ok(get);

            }
        }

        [Route("api/TaiNguyens")]
        [HttpPut]
        public IActionResult PutTaiNguyen(int id, string ten)
        {
            try
            {
                var put = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == false);
                if (put != null)
                {
                    if (ten != null)
                    {
                        put.Ten = ten;
                    }

                    _context.SaveChanges();
                    return Ok(put);
                }
                return BadRequest("Không tìm thấy tài nguyên");
            }
            catch (Exception ex)
            {
                return BadRequest(ModelState);
            }
        }

        [Route("api/TaiNguyens")]
        [HttpPost]
        public IActionResult PostTaiNguyen(BaiGiang_TaiNguyen TaiNguyen)
        {
            try
            {
                if (TaiNguyen != null)
                {
                    TaiNguyen.PhanLoai = false;
                    TaiNguyen.NgayChinhSuaCuoi = DateTime.Now;
                    _context.BaiGiang_TaiNguyen.Add(TaiNguyen);
                    _context.SaveChanges();
                    return Ok(TaiNguyen);
                }
                return BadRequest("Chưa nhập dữ liệu");
            }
            catch
            {
                return BadRequest("Lỗi");
            }
        }

        [Route("api/TaiNguyens/MonHoc")]
        [HttpPut]
        public IActionResult PutTaiNguyenMonHoc(int id, string mon)
        {
            try
            {
                BaiGiang_TaiNguyen tainguyen = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == false);
                if (tainguyen != null)
                {
                    tainguyen.MaMon = mon;
                    _context.SaveChanges();
                    return Ok(tainguyen);
                }
                return BadRequest("Chưa Chọn môn");
            }
            catch (Exception ex)
            {
                return BadRequest("Lỗi");
            }
        }

        [Route("api/TaiNguyens")]
        [HttpDelete]
        public IActionResult DeleteTaiNguyen(int id)
        {
            try
            {
                var delete = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == false);
                if (delete != null)
                {
                    _context.BaiGiang_TaiNguyen.Remove(delete);
                    _context.SaveChanges();
                    return Ok("Xóa thành công");
                }
                return Ok("Không có dữ liệu cần tìm");
            }
            catch (Exception ex)
            {
                return BadRequest("Lỗi");
            }
        }


        [Route("api/BaiGiangs")]
        [HttpGet]
        public IActionResult GetBaiGiang(string mon = null, string ten = null)
        {
            if (mon == null && ten == null)
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                           where c.PhanLoai == true
                           select new
                           {
                               c.Id,
                               c.LoaiFile,
                               c.Ten,
                               c.TenMon,
                               c.NguoiChinhSua,
                               c.NgayChinhSuaCuoi,
                               c.KichThuoc
                           });
                return Ok(get);
            }
            else if (mon != null)
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                           where c.MaMon == mon && c.PhanLoai == false
                           select new
                           {
                               c.Id,
                               c.LoaiFile,
                               c.Ten,
                               c.TenMon,
                               c.NguoiChinhSua,
                               c.NgayChinhSuaCuoi,
                               c.KichThuoc
                           }).OrderBy(x => x.Ten);
                return Ok(get);
            }
            else
            {
                var get = (from c in _context.BaiGiang_TaiNguyen
                           where c.Ten.Contains(ten) && c.PhanLoai == false
                           select new
                           {
                               c.Id,
                               c.LoaiFile,
                               c.Ten,
                               c.TenMon,
                               c.NguoiChinhSua,
                               c.NgayChinhSuaCuoi,
                               c.KichThuoc
                           }).OrderBy(x => x.Ten);
                return Ok(get);

            }
        }

        [Route("api/BaiGiangs")]
        [HttpPut]
        public IActionResult PutBaiGiang(int id, string ten)
        {
            try
            {
                var put = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == true);
                if (put != null)
                {
                    if (ten != null)
                    {
                        put.Ten = ten;
                    }

                    _context.SaveChanges();
                    return Ok(put);
                }
                return BadRequest("Không tìm thấy tài nguyên");
            }
            catch (Exception ex)
            {
                return BadRequest(ModelState);
            }
        }
        [Route("api/BaiGiangs")]
        [HttpPost]
        public IActionResult PostBaiGiang(BaiGiang_TaiNguyen TaiNguyen)
        {
            try
            {
                if (TaiNguyen != null)
                {
                    TaiNguyen.PhanLoai = true;
                    TaiNguyen.NgayChinhSuaCuoi = DateTime.Now;
                    _context.BaiGiang_TaiNguyen.Add(TaiNguyen);
                    _context.SaveChanges();
                    return Ok(TaiNguyen);
                }
                return BadRequest("Chưa nhập dữ liệu");
            }
            catch
            {
                return BadRequest("Lỗi");
            }
        }
        [Route("api/BaiGiangs/MonHoc")]
        [HttpPut]
        public IActionResult PutBaiGiangMonHoc(int id, string mon)
        {
            try
            {
                BaiGiang_TaiNguyen tainguyen = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == true);
                if (tainguyen != null)
                {
                    tainguyen.MaMon = mon;
                    _context.SaveChanges();
                    return Ok(tainguyen);
                }
                return BadRequest("Chưa Chọn môn");
            }
            catch (Exception ex)
            {
                return BadRequest("Lỗi");
            }
        }

        [Route("api/BaiGiangs")]
        [HttpDelete]
        public IActionResult DeleteBaiGiang(int id)
        {
            try
            {
                var delete = _context.BaiGiang_TaiNguyen.SingleOrDefault(n => n.Id == id && n.PhanLoai == true);
                if (delete != null)
                {
                    _context.BaiGiang_TaiNguyen.Remove(delete);
                    _context.SaveChanges();
                    return Ok("Xóa thành công");
                }
                return Ok("Không có dữ liệu cần tìm");
            }
            catch (Exception ex)
            {
                return BadRequest("Lỗi");
            }
        }

        [Route("api/PheDuyet")]
        [HttpPost]
        public IActionResult GuiPheDuyet(int id)
        {
            try
            {
                var duyet = _context.TaiLieu.Where(n => n.Id == id);
                if (duyet != null)
                {
                    TaiLieu taiLieu = new TaiLieu
                    {
                        Id = id,
                        NgayGui = DateTime.Now,
                        TinhTrang = "Chờ phê duyệt"
                    };
                    _context.TaiLieu.Add(taiLieu);
                    _context.SaveChanges();
                    return Ok(taiLieu);
                }
                return BadRequest("Không tìm thấy bài giảng");
            }
            catch (Exception ex)
            {
                return BadRequest("Lỗi");
            }
        }
    }
}