using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.EntityFrameworkCore;
using PetFamily.Application.Abstractions;
using PetFamily.Application.Database;
using PetFamily.Application.Dtos;
using PetFamily.Application.Extensions;
using PetFamily.Domain.Shared;

namespace PetFamily.Application.Volunteers.Queries.GetPetById
{
    public class GetPetByIdHandler(
        IReadDbContext readDbContext,
        IValidator<GetPetByIdQuery> validator) : IQueryHandler<PetDto, GetPetByIdQuery>
    {
        public async Task<Result<PetDto, ErrorList>> Handle(GetPetByIdQuery query,
            CancellationToken cancellationToken = default)
        {
            var validationResult = await validator.ValidateAsync(query, cancellationToken);
            if (validationResult.IsValid == false)
                return validationResult.ToList();

            var pet = await readDbContext.Pets.FirstOrDefaultAsync(p => p.Id == query.PetId, cancellationToken);
            if (pet is null)
                return Errors.General.NotFound(query.PetId).ToErrorList();

            return pet;
        }
    }
}