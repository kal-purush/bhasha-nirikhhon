using AutoMapper;
using DAL.Data.Configurations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests
{
    public static class MapperConfig
    {
        public static IMapper Initialize()
        {
            var mappingConfig = new MapperConfiguration(mc =>
            {
                mc.AddProfile(new TeamMemberConfiguration());
            });

            return mappingConfig.CreateMapper();           
        }
    }
}