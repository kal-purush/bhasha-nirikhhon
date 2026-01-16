using AutoMapper;
using studentprojectapi.GeneratedModels;
using studentprojectapi.Models;



namespace studentprojectapi.Mappings
{


    public class MappingProfile : Profile
    {

        public MappingProfile()
        {
            CreateMap<employee, PersonDTO>().ReverseMap();
            CreateMap<DepartmentDTO, department>().ReverseMap();
            CreateMap<assignment, AssignmentDTO>().ReverseMap();
        }




    }


}