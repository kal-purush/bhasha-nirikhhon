using System.Linq;
using System.Threading.Tasks;
using Api.Dtos;
using Core.Entities.Auth;
using Core.Interfaces;
using Core.Specifications;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    public class AuthController : BaseApiController
    {
        private readonly IGenericRepository<User> _userRepo;
        private readonly IGenericRepository<UserRole> _userRoleRepo;
        private readonly IGenericRepository<UserPoint> _userPointsRepo;
        private readonly ITokenService _tokenService;

        public AuthController(
            IGenericRepository<User> userRepo,
            IGenericRepository<UserRole> userRoleRepo,
            IGenericRepository<UserPoint> userPointsRepo,
            ITokenService tokenService)
        {
            _userRepo = userRepo;
            _userRoleRepo = userRoleRepo;
            _userPointsRepo = userPointsRepo;
            _tokenService = tokenService;
        }
        

        [HttpPost ( "login" )]
        public async Task<ActionResult<LoginUserDto>> LoginAsync( LoginDto loginDto )
        {
            var spec = new UserSpecification ( loginDto.Login );
            var user = await _userRepo.GetEntityWithSpecAsync ( spec );
            
            
            if( user == null ) return Unauthorized ();
            if (user.Password != loginDto.Password) return Unauthorized ();


            var userWithRoleSpec = new UserWithRoleSpecification ( user.Id );
            var userWithPointSpec = new UserWithPointsSpecification ( user.Id );

            var userRole = await _userRoleRepo.GetEntityWithSpecAsync ( userWithRoleSpec );
            var userPoints = await _userPointsRepo.ListAsync ( userWithPointSpec );

            
            return new LoginUserDto
            {
                PointNames = userPoints.Select ( x => x.Point.Name ),
                Role = userRole.Role.Name,
                Token = _tokenService.CreateToken ( user )
            };
        }
    }
}