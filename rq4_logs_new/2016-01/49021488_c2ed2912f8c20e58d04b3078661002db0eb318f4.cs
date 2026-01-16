using AutoMapper;
using StartUpMentor.Model;
using StartUpMentor.Model.Common;
using StartUpMentor.UI.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace StartUpMentor.UI.App_Start
{
    public class UIMappingConfiguration : Profile
    {
        protected override void Configure()
        {
            Mapper.CreateMap<IAnswer, AnswerViewModel>().ReverseMap();
            Mapper.CreateMap<Answer, AnswerViewModel>().ReverseMap();

            Mapper.CreateMap<IField, FieldViewModel>().ReverseMap();
            Mapper.CreateMap<Field, FieldViewModel>().ReverseMap();

            Mapper.CreateMap<IInfo, InfoViewModel>().ReverseMap();
            Mapper.CreateMap<Info, InfoViewModel>().ReverseMap();

            Mapper.CreateMap<IQuestion, QuestionViewModel>().ReverseMap();
            Mapper.CreateMap<Question, QuestionViewModel>().ReverseMap();

            Mapper.CreateMap<IApplicationUser, UserViewModel>().ReverseMap();
            Mapper.CreateMap<StartUpMentor.Model.ApplicationUser, UserViewModel>().ReverseMap();

            Mapper.CreateMap<IVideo, VideoViewModel>().ReverseMap();
            Mapper.CreateMap<Video, VideoViewModel>().ReverseMap();
        }
    }
}