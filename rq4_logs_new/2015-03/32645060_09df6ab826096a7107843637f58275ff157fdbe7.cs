using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using M = Luis.SessionManagement.WebApi.Models;
using C = Luis.SessionManagement.WebApi.Contracts;

namespace Luis.SessionManagement.WebApi
{
    public static class SessionPresenterMappingExtensions
    {
        public static void Configure()
        {
            Mapper.CreateMap<M.SessionPresenter, C.SessionPresenter>()
                .ForMember(dest => dest.Sequence, opt => opt.Ignore())
                .ForMember(dest => dest.SpeakerName,
                    opt =>
                        opt.ResolveUsing(
                            src => string.Format("{0}, {1}", src.Presenter.LastName, src.Presenter.FirstName)))
                ;
            Mapper.CreateMap<C.SessionPresenter, M.SessionPresenter>()
                .ForMember(dest => dest.Presenter, opt => opt.Ignore())
                .ForMember(dest => dest.Session, opt => opt.Ignore())
                .ForMember(dest => dest.CreateDate, opt => opt.Ignore())
                .ForMember(dest => dest.UpdateDate, opt => opt.Ignore())
                ;
        }

        public static C.SessionPresenter ToContract(this M.SessionPresenter presenter)
        {
            return Mapper.Map<M.SessionPresenter, C.SessionPresenter>(presenter);
        }

        public static M.SessionPresenter ToModel(this C.SessionPresenter presenter)
        {
            return Mapper.Map<C.SessionPresenter, M.SessionPresenter>(presenter);
        }

        public static List<C.SessionPresenter> ToContract(this List<M.SessionPresenter> presenters)
        {
            return Mapper.Map<List<M.SessionPresenter>, List<C.SessionPresenter>>(presenters);
        }

        public static List<M.SessionPresenter> ToModel(this List<C.SessionPresenter> presenters)
        {
            return Mapper.Map<List<C.SessionPresenter>, List<M.SessionPresenter>>(presenters);
        }
    }
}