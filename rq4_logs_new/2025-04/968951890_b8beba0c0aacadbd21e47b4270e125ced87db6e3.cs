using MediatR;
using static Lucky7_Inventory_System_Application.Responses.ServiceResponses;

namespace Lucky7_Inventory_System_Application.Queries.UserQueries;

public class GetAllUsersQuery : IRequest<GetResponse>
{
}