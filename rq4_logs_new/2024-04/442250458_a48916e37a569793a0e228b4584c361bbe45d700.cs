using System;
using System.Threading;
using System.Threading.Tasks;
using AutoMapper;
using KDVManager.Services.Scheduling.Application.Contracts.Persistence;
using KDVManager.Services.Scheduling.Application.Exceptions;
using KDVManager.Services.Scheduling.Domain.Entities;
using MediatR;

namespace KDVManager.Services.Scheduling.Application.Features.Groups.Commands.DeleteGroup;

public class DeleteGroupCommandHandler : IRequestHandler<DeleteGroupCommand>
{
    private readonly IGroupRepository _groupRepository;
    private readonly IMapper _mapper;

    public DeleteGroupCommandHandler(IGroupRepository childRepository, IMapper mapper)
    {
        _groupRepository = childRepository;
        _mapper = mapper;
    }

    public async Task<Unit> Handle(DeleteGroupCommand request, CancellationToken cancellationToken)
    {
        var groupToDelete = await _groupRepository.GetByIdAsync(request.Id);

        if (groupToDelete == null)
        {
            throw new NotFoundException(nameof(Group), request.Id);
        }

        await _groupRepository.DeleteAsync(groupToDelete);

        return Unit.Value;
    }
}