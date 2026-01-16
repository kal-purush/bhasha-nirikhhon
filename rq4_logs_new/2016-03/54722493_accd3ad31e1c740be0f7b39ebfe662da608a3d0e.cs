using System.Collections.Generic;
using System.Linq;
using AutoMapper;
using SimpleInjector;
using WebTests.Models;
using WebTests.Repository;
using WebTests.Services;

namespace WebTests
{
    public static class DependencyConfig
    {
        public static Container Register(Options options)
        {
            var container = new Container();

            container.RegisterSingleton(GetMapper);
            container.RegisterSingleton(() => options);
            container.RegisterSingleton<IFileService, FileService>();
            container.RegisterSingleton<ICardRunResultRepository, CardRunResultRepository>();

            return container;
        }

        private static IMapper GetMapper()
        {
            var mapperConfig =
                new MapperConfiguration(
                    config =>
                    {
                        config.CreateMap<Card, CardRunResultRecord>()
                              .ForMember(dest => dest.CardId, opt => opt.MapFrom(src => src.Id))
                              .ForMember(dest => dest.CardName, opt => opt.MapFrom(src => src.Name))
                              .ForMember(dest => dest.Id, opt => opt.Ignore())
                              .ForMember(dest => dest.Score, opt => opt.Ignore())
                              .ForMember(dest => dest.Timespan, opt => opt.Ignore());
                        config.CreateMap<CardRunResultRecord, Card>()
                              .ForMember(dest => dest.Id, opt => opt.MapFrom(src => src.CardId))
                              .ForMember(dest => dest.Name, opt => opt.MapFrom(src => src.CardName));
                        config.CreateMap<CardRunResult, CardRunResultMap>()
                              .ForMember(dest => dest.Records, opt => opt.MapFrom(src => src.Cards))
                              .AfterMap(
                                  (s, d) =>
                                  {
                                      foreach(var card in d.Records)
                                      {
                                          card.Id = s.Id;
                                          card.Score = s.Score;
                                          card.Timespan = s.Timespan;
                                      }
                                  });
                        config.CreateMap<CardRunResultMap, CardRunResult>()
                              .ForMember(d => d.Cards, opt => opt.MapFrom(src => src.Records))
                              .ForMember(d => d.Id, opt => opt.MapFrom(src => src.Records.First().Id))
                              .ForMember(d => d.Timespan, opt => opt.MapFrom(src => src.Records.First().Timespan))
                              .ForMember(d => d.Score, opt => opt.MapFrom(src => src.Records.First().Score))
                              .ForMember(d => d.IsGoodResult, opt => opt.Ignore());
                        config.CreateMap<List<CardRunResultRecord>, CardRunResult>()
                              .ForMember(d => d.Cards, opt => opt.MapFrom(src => src))
                              .ForMember(d => d.Id, opt => opt.MapFrom(src => src.First().Id))
                              .ForMember(d => d.Timespan, opt => opt.MapFrom(src => src.First().Timespan))
                              .ForMember(d => d.Score, opt => opt.MapFrom(src => src.First().Score))
                              .ForMember(d => d.IsGoodResult, opt => opt.Ignore());
                        config.CreateMap<IEnumerable<CardRunResultRecord>, CardRunResult>()
                              .ForMember(d => d.Cards, opt => opt.MapFrom(src => src))
                              .ForMember(d => d.Id, opt => opt.MapFrom(src => src.First().Id))
                              .ForMember(d => d.Timespan, opt => opt.MapFrom(src => src.First().Timespan))
                              .ForMember(d => d.Score, opt => opt.MapFrom(src => src.First().Score))
                              .ForMember(d => d.IsGoodResult, opt => opt.Ignore());
                    });

            mapperConfig.AssertConfigurationIsValid();

            return mapperConfig.CreateMapper();
        }
    }
}