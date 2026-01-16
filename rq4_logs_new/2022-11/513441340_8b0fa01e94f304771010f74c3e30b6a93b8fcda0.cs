using IPS.UserManagement.Application.Features.Permissions.Converters;
using IPS.UserManagement.Application.Features.Permissions.Models;
using IPS.UserManagement.Domain.Permissions;

namespace IPS.UserManagement.Application.Features.Permissions.Commands;

public class CreatePermissionCommand : IRequest<PermissionQueryModel>
{
    private CreatePermissionCommandModel Request { get; }

    public CreatePermissionCommand(CreatePermissionCommandModel request)
    {
        Request = request;
    }

    private class CreatePermissionCommandHandler : IRequestHandler<CreatePermissionCommand, PermissionQueryModel>
    {
        private readonly IPermissionRepository _repository;
        private readonly PermissionRequestConverter _requestConverter;
        private readonly PermissionConverter _resourceConverter;

        public CreatePermissionCommandHandler(
            IPermissionRepository repository,
            PermissionRequestConverter requestConverter,
            PermissionConverter resourceConverter)
        {
            _repository = repository;
            _requestConverter = requestConverter;
            _resourceConverter = resourceConverter;
        }

        public async Task<PermissionQueryModel> Handle(CreatePermissionCommand command, CancellationToken cancel)
        {
            var request = _requestConverter.ToDomain(command.Request);
            var resource = await _repository.CreateAsync(request, cancel);
            var model = _resourceConverter.ToModel(resource);
            return model;
        }
    }
}