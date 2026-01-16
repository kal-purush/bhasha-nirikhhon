using AutoMapper;
using CleanArchitecture.Application.Commands.Family;
using CleanArchitecture.Domain.Contracts.IRepositories;
using CleanArchitecture.Domain.Models;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CleanArchitecture.Application.CommandHandlers
{
    public class FamilyCommandHandlers : IRequestHandler<CreateFamilyCommand, Guid>
                                        , IRequestHandler<UpdateFamilyCommand, int>
                                        , IRequestHandler<DeleteFamilyCommand, int>
                                        , IRequestHandler<UpdateByLastnameFamilyCommand, int>
    {
        private readonly IMapper _mapper;
        private readonly IFamilyRepository<FamilyModel> _repo;

        public FamilyCommandHandlers(IMapper mapper, IFamilyRepository<FamilyModel> repo)
        {
            _mapper = mapper;
            _repo = repo;
        }


        public async Task<Guid> Handle(CreateFamilyCommand request, CancellationToken cancellationToken)
        {
            var data = _mapper.Map<FamilyModel>(request);

            return await _repo.Create(data);
        }

        public async Task<int> Handle(UpdateFamilyCommand request, CancellationToken cancellationToken)
        {

            return await _repo.Update(request.Id, request);
        }

        public async Task<int> Handle(DeleteFamilyCommand request, CancellationToken cancellationToken)
        {
            return await _repo.Delete(request.Id);
        }

        public async Task<int> Handle(UpdateByLastnameFamilyCommand request, CancellationToken cancellationToken)
        {
            return await _repo.UpdateByLastname(request.Id, request.Lastname);
        }
    }
}