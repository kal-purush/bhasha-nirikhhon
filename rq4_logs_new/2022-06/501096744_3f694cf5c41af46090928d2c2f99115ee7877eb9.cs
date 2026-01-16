using AutoMapper;
using FoodZone.API.Models;
using FoodZone.Models.Common;

namespace FoodZone.API.Configurations
{
    public class MapperInitializer : Profile
    {
        public MapperInitializer()
        {
            CreateMap<Feedback, FeedbackDTO>().ReverseMap();
            CreateMap<Food, FoodDTO>().ReverseMap();
            CreateMap<Menu, MenuDTO>().ReverseMap();
            CreateMap<Payment, PaymentDTO>().ReverseMap();
            CreateMap<Reservation, ReservationDTO>().ReverseMap();
            CreateMap<ReservationDetail, ReservationDetailDTO>().ReverseMap();
            CreateMap<Salary, SalaryDTO>().ReverseMap();
            CreateMap<Table, TableDTO>().ReverseMap();

        }
    }
}