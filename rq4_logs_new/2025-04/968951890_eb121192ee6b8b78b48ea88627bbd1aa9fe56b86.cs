using Lucky7_Inventory_System_Application.Constants;
using Lucky7_Inventory_System_Application.Interfaces;
using Lucky7_Inventory_System_Domain.Entities;
using MediatR;
using static Lucky7_Inventory_System_Application.Responses.ServiceResponses;

namespace Lucky7_Inventory_System_Application.Queries.StatusQueries.Handlers;

public class GetAllStatusQueryHandler : IRequestHandler<GetAllStatusQuery, GetResponse>
{
    private readonly IGenericRepository<Status> _repository;

    public GetAllStatusQueryHandler(IGenericRepository<Status> repository)
    {
        _repository = repository;
    }

    public async Task<GetResponse> Handle(GetAllStatusQuery request, CancellationToken cancellationToken)
    {
        try
        {
            var statuses = await _repository.GetAll();
            if (statuses == null)
            {
                return new GetResponse(true, null, "No statuses found", StatusResponse.notfound);
            }

            return new GetResponse(true, statuses, "Statuses were Successfully Retrieved", StatusResponse.success);
        }
        catch (Exception ex)
        {
            return new GetResponse(true, null, ex.Message, StatusResponse.unhandled);
        }
    }
}