using AutoMapper;
using VanKassa.Domain.Dtos;
using VanKassa.Domain.ViewModels;

namespace VanKassa.Backend.Core.AutoMappersConfig.Converters;

public class OutletDtoToOutletViewModelConverter : ITypeConverter<OutletDto, EmployeeOutletViewModel>
{
    public EmployeeOutletViewModel Convert(OutletDto source, EmployeeOutletViewModel destination, ResolutionContext context)
        => new()
        {
            Id = source.Id,
            Address = string.Join(", ", source.City, source.Street, source.StreetNumber)
        };
}

public class OutletViewModelToDtoConverter : ITypeConverter<EmployeeOutletViewModel, OutletDto>
{
    public OutletDto Convert(EmployeeOutletViewModel source, OutletDto destination, ResolutionContext context)
    {
        var addressTypes = source.Address.Split(", ").ToList();

        return new OutletDto
        {
            Id = source.Id, 
            City = addressTypes[0],
            Street = addressTypes[1],
            StreetNumber = addressTypes[2]
        };
    }       
}