using AutoMapper;
using Com.Bateeq.Service.Warehouse.Lib.Models.Expeditions;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.ExpeditionViewModel;
using System;
using System.Collections.Generic;
using System.Text;

namespace Com.Bateeq.Service.Warehouse.Lib.AutoMapperProfiles
{
    public class ExpeditionProfile : Profile
    {
        public ExpeditionProfile()
        {
            CreateMap<Expedition, ExpeditionViewModel>()
                .ForMember(d => d._id, opt => opt.MapFrom(s => s.Id))
                .ForMember(d => d.remark, opt => opt.MapFrom(s => s.Remark))

                
                .ForPath(d => d.expeditionService._id, opt => opt.MapFrom(s => s.ExpeditionServiceId))
                .ForPath(d => d.expeditionService.code, opt => opt.MapFrom(s => s.ExpeditionServiceCode))
                .ForPath(d => d.expeditionService.name, opt => opt.MapFrom(s => s.ExpeditionServiceName))

                .ForMember(d => d.items, opt => opt.MapFrom(s => s.Items))
                .ReverseMap();

            CreateMap<ExpeditionItem, ExpeditionItemViewModel>()
                .ForMember(d => d._id, opt => opt.MapFrom(s => s.Id))
                .ForMember(d => d.weight, opt => opt.MapFrom(s => s.Weight))
                .ForPath(d => d.spkDocsViewModel.Id, opt => opt.MapFrom(s => s.SPKDocsId))
                .ForPath(d => d.spkDocsViewModel.code, opt => opt.MapFrom(s => s.Code))
                .ForPath(d => d.spkDocsViewModel.isDistributed, opt => opt.MapFrom(s => s.IsDistributed))
                .ForPath(d => d.spkDocsViewModel.isDraft, opt => opt.MapFrom(s => s.IsDraft))
                .ForPath(d => d.spkDocsViewModel.isReceived, opt => opt.MapFrom(s => s.IsReceived))
                .ForPath(d => d.spkDocsViewModel.packingList, opt => opt.MapFrom(s => s.PackingList))
                .ForPath(d => d.spkDocsViewModel.password, opt => opt.MapFrom(s => s.Password))
                .ForPath(d => d.spkDocsViewModel.reference, opt => opt.MapFrom(s => s.Reference))
                //.ForPath(d => d.spkDocsViewModel.Weight, opt => opt.MapFrom(s => s.Weight))
                .ForPath(d => d.spkDocsViewModel.destination._id, opt => opt.MapFrom(s => s.DestinationId))
                .ForPath(d => d.spkDocsViewModel.destination.code, opt => opt.MapFrom(s => s.DestinationCode))
                .ForPath(d => d.spkDocsViewModel.destination.name, opt => opt.MapFrom(s => s.DestinationName))
                .ForPath(d => d.spkDocsViewModel.source._id, opt => opt.MapFrom(s => s.SourceId))
                .ForPath(d => d.spkDocsViewModel.source.code, opt => opt.MapFrom(s => s.SourceCode))
                .ForPath(d => d.spkDocsViewModel.source.name, opt => opt.MapFrom(s => s.SourceName))
                .ForMember(d => d.details, opt => opt.MapFrom(s => s.Details))
                .ReverseMap();

            CreateMap<ExpeditionDetail, ExpeditionDetailViewModel>()
                .ForMember(d => d._id, opt => opt.MapFrom(s => s.Id))

                .ForPath(d => d.item._id, opt => opt.MapFrom(s => s.ItemId))
                .ForPath(d => d.item.code, opt => opt.MapFrom(s => s.ItemCode))
                .ForPath(d => d.item.name, opt => opt.MapFrom(s => s.ItemName))
                .ForPath(d => d.item.articleRealizationOrder, opt => opt.MapFrom(s => s.ArticleRealizationOrder))
                .ForPath(d => d.item.domesticCOGS, opt => opt.MapFrom(s => s.DomesticCOGS))
                .ForPath(d => d.item.domesticRetail, opt => opt.MapFrom(s => s.DomesticRetail))
                .ForPath(d => d.item.domesticSale, opt => opt.MapFrom(s => s.DomesticSale))
                .ForPath(d => d.item.domesticWholesale, opt => opt.MapFrom(s => s.DomesticWholesale))
                .ForPath(d => d.item.size, opt => opt.MapFrom(s => s.Size))
                .ForPath(d => d.item.uom, opt => opt.MapFrom(s => s.Uom))
                .ReverseMap();
        }
    }
}