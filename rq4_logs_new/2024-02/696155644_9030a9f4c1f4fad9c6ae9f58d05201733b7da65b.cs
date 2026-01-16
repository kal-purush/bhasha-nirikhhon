using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using PMAPI.Errors;
using PMAPI.Helper;
using PMAPI.Models.OrgUser;
using PMAPI.Models.Query;
using PMAPI.Services.IServices;
using PMCore.Configuration;
using PMDB.Models;
using System.Linq.Dynamic.Core;
using System.Net;

namespace PMAPI.Controllers
{
	[Route("api/[controller]")]
	[ApiController]
	[Authorize(Roles = AppConst.Role.Organization)]
	public class OrgUsersController : BaseController
	{
		private readonly PmdbContext _context;
		private readonly IMapper _mapper;
		private readonly IAuthService _authService;

		public OrgUsersController(PmdbContext context, IMapper mapper, IAuthService authService)
		{
			_context = context;
			_mapper = mapper;
			_authService = authService;
		}

		[HttpPost(nameof(QueryUsers))]
		public async Task<ActionResult<QueryViewModel<List<OrgUserViewModel>>>> QueryUsers(QueryModel<OrgUserQueryModel> model)
		{
			if (model.Filter == null || !await _authService.CheckOrgAdmin(model.Filter.RootDID, _uid))
			{
				throw new RestException(HttpStatusCode.Forbidden);
			}

			var listQuery = _context.TbOrgUsers.Where(x => x.TbOrgDeptUsers.Any(du => du.DidNavigation.RootDid == model.Filter.RootDID));

			if (model.Filter != null)
			{
				if (!string.IsNullOrWhiteSpace(model.Filter.Name))
				{
					listQuery = listQuery.Where(x => x.Name.Contains(model.Filter.Name));
				}

				if (!string.IsNullOrWhiteSpace(model.Filter.EMail))
				{
					listQuery = listQuery.Where(x => x.Email.Contains(model.Filter.EMail));
				}
			}

			var count = listQuery.Count();

			if (string.IsNullOrWhiteSpace(model.Sort))
			{
				listQuery = listQuery.OrderBy(x => x.Name);
			}
			else
			{
				listQuery = listQuery.OrderBy(model.OrderBy);
			}

			listQuery = listQuery.Skip(model.Skip).Take(model.PageSize);

			return new QueryViewModel<List<OrgUserViewModel>>
			{
				Data = await _mapper.ProjectTo<OrgUserViewModel>(listQuery).ToListAsync(),
				PageCount = model.PageCount(count)
			};
		}

		// GET: api/OrgUsers/5
		[HttpGet("{id}")]
		public async Task<ActionResult<TbOrgUser>> GetTbOrgUser(string id)
		{
			if (_context.TbOrgUsers == null)
			{
				return NotFound();
			}
			var tbOrgUser = await _context.TbOrgUsers.FindAsync(id);

			if (tbOrgUser == null)
			{
				return NotFound();
			}

			return tbOrgUser;
		}

		// PUT: api/OrgUsers/5
		// To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
		[HttpPut("{id}")]
		public async Task<IActionResult> PutTbOrgUser(string id, TbOrgUser tbOrgUser)
		{
			if (id != tbOrgUser.Uid)
			{
				return BadRequest();
			}

			_context.Entry(tbOrgUser).State = EntityState.Modified;

			try
			{
				await _context.SaveChangesAsync();
			}
			catch (DbUpdateConcurrencyException)
			{
				if (!TbOrgUserExists(id))
				{
					return NotFound();
				}
				else
				{
					throw;
				}
			}

			return NoContent();
		}

		// POST: api/OrgUsers
		// To protect from overposting attacks, see https://go.microsoft.com/fwlink/?linkid=2123754
		[HttpPost]
		public async Task<ActionResult<TbOrgUser>> PostTbOrgUser(TbOrgUser tbOrgUser)
		{
			if (_context.TbOrgUsers == null)
			{
				return Problem("Entity set 'PmdbContext.TbOrgUsers'  is null.");
			}
			_context.TbOrgUsers.Add(tbOrgUser);
			try
			{
				await _context.SaveChangesAsync();
			}
			catch (DbUpdateException)
			{
				if (TbOrgUserExists(tbOrgUser.Uid))
				{
					return Conflict();
				}
				else
				{
					throw;
				}
			}

			return CreatedAtAction("GetTbOrgUser", new { id = tbOrgUser.Uid }, tbOrgUser);
		}

		// DELETE: api/OrgUsers/5
		[HttpDelete("{id}")]
		public async Task<IActionResult> DeleteTbOrgUser(string id)
		{
			if (_context.TbOrgUsers == null)
			{
				return NotFound();
			}
			var tbOrgUser = await _context.TbOrgUsers.FindAsync(id);
			if (tbOrgUser == null)
			{
				return NotFound();
			}

			_context.TbOrgUsers.Remove(tbOrgUser);
			await _context.SaveChangesAsync();

			return NoContent();
		}

		private bool TbOrgUserExists(string id)
		{
			return (_context.TbOrgUsers?.Any(e => e.Uid == id)).GetValueOrDefault();
		}
	}
}