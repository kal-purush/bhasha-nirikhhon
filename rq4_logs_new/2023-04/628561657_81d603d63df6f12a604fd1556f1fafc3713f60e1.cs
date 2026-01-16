using JKEY_INTERNAL.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System.Collections;
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
        public IActionResult GetChildrenIds()
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
                arrayList1.Add(childrenIds);
            }
            foreach (var menu in parentMenu)
            {
                arrayList1.Add(menu);
            }


            return new JsonResult(arrayList1);
        }

        // GET: api/<MenusController>
        [HttpGet]
        public IActionResult GetMenu()
        {
            List<Menu> Items = new List<Menu>();
            Items = _context.Menus.ToList();
            int Count = Items.Count();
            return Ok(new { Items, Count });
        }



        // GET api/<MenusController>/5
        [HttpGet("{id}")]
        public IActionResult GetMenuById([FromRoute] string id)
        {
            Menu userProfile = _context.Menus.SingleOrDefault(x => x.Id.Equals(id));
            List<Menu> Items = new List<Menu>();
            if (userProfile != null)
            {
                Items.Add(userProfile);
            }
            int Count = Items.Count();
            return Ok(new { Items, Count });
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
                    //UserCreated=date
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
        //[HttpPut("{id}")]
        //public IActionResult Edit(string id, [FromBody] Menu menu)
        //{
        //    var oldData = _context.Menus.Find(id);

        //    if (oldData == null)
        //    {
        //        return NotFound();
        //    }

        //    oldData.Name = newData.Property1;
        //    oldData.Property2 = newData.Property2;
        //    // Update other properties as needed

        //    _dbContext.SaveChanges();

        //    return new JsonResult(oldData);
        //}


        // DELETE api/<MenusController>/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}
