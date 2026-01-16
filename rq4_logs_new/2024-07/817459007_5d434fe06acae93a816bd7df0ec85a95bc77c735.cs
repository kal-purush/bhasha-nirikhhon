using BlazorEcommerce.Shared;
using MediatR;

namespace BlazorEcommerce.Server;

public class CreateLocationCommand : IRequest<UpdateLocationDTO>
{
    public UpdateLocationDTO locationDTO { get; set; }
    public CreateLocationCommand(UpdateLocationDTO LocationDTO)
    {
        locationDTO = LocationDTO;
    }
}

public class CreateLocationCommandHandler : IRequestHandler<CreateLocationCommand, UpdateLocationDTO>
{
    private readonly AppDbContext _dbContext;
    public CreateLocationCommandHandler(AppDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    public async Task<UpdateLocationDTO> Handle(CreateLocationCommand request, CancellationToken cancellationToken)
    {
        var locationToAdd = new Location
        {
            Name = request.locationDTO.Name,
            Address = request.locationDTO.Address
        };

        await _dbContext.Locations.AddAsync(locationToAdd);
        await _dbContext.SaveChangesAsync();

        return request.locationDTO;
    }
}