using dbcontext;
using dbcontext.Classes;
using Linktindr_be.Dto;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Linktindr_be.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UserController
    {
        private OurContext OC;

        public UserController(OurContext OC)
        {
            this.OC = OC;
        }

        public LoginResponseDto login([FromBody] LoginRequestDto dto)
        {
            // Check medewerker
            Medewerker? m = OC.Medewerker.FirstOrDefault(u => u.Email.Equals(dto.Email) && u.Password.Equals(dto.Email));
            if (m != null) {
            }

            // Check opdrachtgever
            Opdrachtgever? o = OC.Opdrachtgever.FirstOrDefault(u => u.Email.Equals(dto.Email) && u.Password.Equals(dto.Email));
            if (o != null)
            {
            }

            // Check talentmanager
            TalentManager? t = OC.TalentManager.FirstOrDefault(u => u.Email.Equals(dto.Email) && u.Password.Equals(dto.Email));
            if (t != null)
            {
            }

            return new LoginResponseDto();
        }

    }
}