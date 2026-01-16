using System;
using System.Threading;
using System.Threading.Tasks;
using AtmosAQ.Application.Interfaces;
using MediatR;

namespace AtmosAQ.Application.MeasurementsData.Queries
{
    public class GetMeasurementsQuery : IRequest<GetMeasurementsDto>
    {
        public DateTime DateFrom { get; set; }
        
        public DateTime DateTo { get; set; }
        
        public string City { get; set; }

        public class GetMeasurementsQueryHandler : IRequestHandler<GetMeasurementsQuery, GetMeasurementsDto>
        {
            private readonly IDataService _dataService;

            public GetMeasurementsQueryHandler(IDataService dataService)
            {
                _dataService = dataService;
            }
            
            public async Task<GetMeasurementsDto> Handle(GetMeasurementsQuery request, CancellationToken cancellationToken)
            {
                var result = await _dataService.GetMeasurementsData(request);

                return result;
            }
        }
    }
}