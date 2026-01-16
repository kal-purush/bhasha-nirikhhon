using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace webtesting
{
    public class AutoMapper : Profile
    {
        public AutoMapper()
        {
            CreateMap<user, GetCharacterDto>();
            CreateMap<AddCharacterDto, user>();
        }
    }
}