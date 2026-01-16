using Assets.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Linq;

namespace Assets.Controllers
{
    [Route("api/[controller]")]
    public class TenantsController : Controller
    {
        private readonly Entities.AssetsDbContext _db;

        public TenantsController(Entities.AssetsDbContext db)
        {
            _db = db;
        }

        [HttpGet("my")]
        [Authorize(Policy = "Authenticated")]
        public IQueryable<Tenant> Get()
        {
            return from user in _db.Users
                   where user.UserName == User.Identity.Name
                   from tenantRole in user.TenantRoles
                   let tenant = tenantRole.Tenant
                   where tenant.DeletedDate == null
                   select new Tenant
                   {
                       Href = Url.Action("Get", new { area = tenant.Area }),
                       Version = tenant.Version,
                       Area = tenant.Area,
                       Name = tenant.Name
                   };
        }
    }
}