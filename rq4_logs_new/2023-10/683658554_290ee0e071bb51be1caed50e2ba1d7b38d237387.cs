using MediatR;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Text;
using YourScheduler.BusinessLogic.Services.Interfaces;
using YourScheduler.BusinessLogic.Services.Settings;
using YourScheduler.Infrastructure.Repositories.Interfaces;

namespace YourScheduler.BusinessLogic.Commands.AcceptTeamInvitation
{
    public class AcceptTeamInvitationCommandHandler : IRequestHandler<AcceptTeamInvitationCommand>
    {
        private readonly ITeamMemberRepository _teamMemberRepository;
        private readonly JwtSettings _jwtSettings;

        public AcceptTeamInvitationCommandHandler(ITeamMemberRepository teamMemberRepository, IOptions<JwtSettings> jwtSettings)
        {
            _teamMemberRepository = teamMemberRepository;
            _jwtSettings = jwtSettings.Value;
        }
        public async Task Handle(AcceptTeamInvitationCommand request, CancellationToken cancellationToken)
        {
            var tokenHandler = new JwtSecurityTokenHandler();
            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_jwtSettings.SecretKey));

            tokenHandler.ValidateToken(request.Token, new TokenValidationParameters
            {
                IssuerSigningKey = key,
                ValidateIssuer = false,
                ValidateAudience = false,
                ValidateLifetime = true,
                ClockSkew = TimeSpan.Zero
            }, out SecurityToken validatedToken);

            var claims = (validatedToken as JwtSecurityToken)?.Claims;
            if (claims != null)
            {
                string userId = claims.FirstOrDefault(c => c.Type == "UserIdClaim")?.Value ?? throw new Exception("UserIdClaim is null!");
                string TeamId = claims.FirstOrDefault(c => c.Type == "TeamIdClaim")?.Value ?? throw new Exception("TeamIdClaim is null!");


                await _teamMemberRepository.AddTeamMemberAsync(int.Parse(userId), int.Parse(TeamId));
            }
        }
    }
} 