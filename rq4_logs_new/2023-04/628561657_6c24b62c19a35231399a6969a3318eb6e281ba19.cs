using Azure;
using JKEY_INTERNAL.Models;
using JKEY_INTERNAL.Models.CustomModel;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System.Collections;
using System.Collections.Generic;
using System.Web.Helpers;
using System.Xml.Linq;


// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace JKEY_INTERNAL.Controllers.Api
{
    [Route("api/menu")]
    [ApiController]
    public class MenusController : ControllerBase
    {
        private readonly JkeyInternalContext _context;

        public MenusController(JkeyInternalContext context)
        {
            _context = context;

        }
        [HttpGet("children")]
        public ServiceResponse GetChildrenIds()
        {
            try
            {
                var parentMenu = _context.Menus
                                .Where(d => d.ParentId == "0" && d.Active == true)
                                .ToList();
                ArrayList arrayList1 = new ArrayList();

                foreach (var menu in parentMenu)
                {
                    var childrenIds = _context.Menus
                    .Where(d => d.ParentId == menu.Id && d.Active == true)
                    .ToList();
                    arrayList1.Add(menu);
                    if (childrenIds.Count > 0)
                    {
                        arrayList1.Add(childrenIds);
                    }

                }
                return new ServiceResponse
                {
                    Data = arrayList1,
                    Success = true,
                    StatusCode = StatusCodes.Status200OK,
                };
            }
            catch (Exception ex)
            {
                return new ServiceResponse
                {
                    StatusCode = StatusCodes.Status500InternalServerError,
                    Success = false,
                    Data = ex
                };
            }

        }

        // GET: api/<MenusController>
        [HttpGet]
        public ServiceResponse GetMenu()
        {
            try
            {
                List<Menu> Items = new List<Menu>();
                Items = _context.Menus.ToList();
                int Count = Items.Count();
                return new ServiceResponse
                {
                    Data = new { Items, Count },
                    Success = true,
                    StatusCode = StatusCodes.Status200OK,
                };

            }
            catch (Exception ex)
            {
                return new ServiceResponse
                {
                    StatusCode = StatusCodes.Status500InternalServerError,
                    Success = false,
                    Data = ex
                };
            }

        }

        // GET api/<MenusController>/5
        [HttpGet("{id}")]
        public ServiceResponse GetMenuById([FromRoute] string id)
        {
            try
            {
                Menu userProfile = _context.Menus.SingleOrDefault(x => x.Id.Equals(id));
                List<Menu> Items = new List<Menu>();
                if (userProfile != null)
                {
                    Items.Add(userProfile);
                }
                int Count = Items.Count();
                return new ServiceResponse
                {
                    Data = new { Items, Count },
                    Success = true,
                    StatusCode = StatusCodes.Status200OK,
                };

            }
            catch (Exception ex)
            {
                return new ServiceResponse
                {
                    StatusCode = StatusCodes.Status500InternalServerError,
                    Success = false,
                    Data = ex
                };
            }

        }

        // POST api/<MenusController>
        [HttpPost]
        public async Task<IActionResult> InsertMenu([FromBody] Menu menu)
        {
            try
            {
                // Tạo một đối tượng mới cho dữ liệu cần chèn
                var newData = new Menu()
                {
                    Id = menu.Id,
                    Name = menu.Name,
                    Icon = menu.Icon,
                    ParentId = menu.ParentId,
                    Order = menu.Order,
                    Link = menu.Link,
                    PageId = menu.PageId,
                    Active = menu.Active,
                    UserCreated = menu.UserCreated,
                    UserModified = menu.UserModified,
                    DateCreated = DateTime.UtcNow,
                    DateModified = DateTime.UtcNow
                };

                // Thêm dữ liệu vào cơ sở dữ liệu
                _context.Menus.Add(newData);
                await _context.SaveChangesAsync();

                // Trả về đối tượng dữ liệu đã được chèn dưới dạng JSON
                return new JsonResult(newData);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }


        // PUT api/<MenusController>/5
        [HttpPut("{id}")]
        public ServiceResponse Edit(string id, [FromBody] Menu menu)
        {
            try
            {
                var oldData = _context.Menus.Find(id);

                if (oldData == null)
                {
                    NotFound();
                }

                oldData.Name = menu.Name;
                oldData.Icon = menu.Icon;
                oldData.ParentId = menu.ParentId;
                oldData.Order = menu.Order;
                oldData.Link = menu.Link;
                oldData.PageId = menu.PageId;
                oldData.Active = menu.Active;
                oldData.UserCreated = menu.UserCreated;
                oldData.UserModified = menu.UserModified;
                oldData.DateCreated = DateTime.UtcNow;
                oldData.DateModified = DateTime.UtcNow;
                // Update other properties as needed

                _context.SaveChanges();

                return new ServiceResponse
                {
                    Data = oldData,
                    Success = true,
                    StatusCode = StatusCodes.Status200OK,
                };
            }
            catch (Exception ex)
            {
                return new ServiceResponse
                {
                    StatusCode = StatusCodes.Status500InternalServerError,
                    Success = false,
                    Data = ex
                };
            }

        }


        // DELETE api/<MenusController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
