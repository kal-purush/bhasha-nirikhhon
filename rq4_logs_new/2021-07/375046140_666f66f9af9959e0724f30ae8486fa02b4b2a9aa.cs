using System.Threading.Tasks;
using Api.Dtos;
using Api.Extensions;
using Core.Entities.Halko;
using Core.Entities.Identity;
using Core.Enums;
using Core.Interfaces;
using Core.Specifications;
using Infrastructure.Data;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi.Extensions;

namespace Api.Controllers
{
    public class ParticipantController : BaseApiController
    {
        #region Private Members
        
        private readonly IGenericRepository<Core.Entities.Halko.Point> _pointRepo;
        private readonly IGenericRepository<ParticipantPoint> _participantRepo;
        private readonly HalkoContext _context;
        private readonly UserManager<AppUser> _userManager;
        
        #endregion

        #region Constructor
        
        public ParticipantController(
            IGenericRepository<Core.Entities.Halko.Point> pointRepo,
            IGenericRepository<ParticipantPoint> participantRepo,
            HalkoContext context,
            UserManager<AppUser> userManager)
        {
            _pointRepo = pointRepo;
            _participantRepo = participantRepo;
            _context = context;
            _userManager = userManager;
        }
        
        #endregion

        [Authorize]
        [HttpPost]
        public async Task<ActionResult> SaveParticipant(ParticipantCreateDto participantCreateDto)
        {
            #region Validate
            
            var userRole = await _userManager.FindByNameByClaimsPrincipleUserRoleAsync ( User );

            if( userRole != EUserRole.Admin.GetDisplayName() )
                return Unauthorized();

            if( string.IsNullOrWhiteSpace ( participantCreateDto.Initial ) ||
                string.IsNullOrWhiteSpace ( participantCreateDto.PointName ) )
                return BadRequest();
            
            #endregion

            var pointSpec = new PointSpecification ( participantCreateDto.PointName );
            var point = await _pointRepo.GetEntityWithSpecAsync ( pointSpec );
            if( point == null ) return BadRequest();

            
            var participantSpec = new ParticipantSpecification ( participantCreateDto.Initial, point.Id );
            var participant = await _participantRepo.GetEntityWithSpecAsync ( participantSpec );
            if (participant != null) return BadRequest();
            

            var participantToCreate = new ParticipantPoint
            {
                FirstName = participantCreateDto.FirstName,
                LastName = participantCreateDto.LastName,
                Initial = participantCreateDto.Initial,
                PointId = point.Id,
            };

            
            await _context.AddAsync ( participantToCreate );
            var result = await _context.SaveChangesAsync();


            return result > 0 ? Ok() : BadRequest();
        }
        
    }
}