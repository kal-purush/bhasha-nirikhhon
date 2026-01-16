using System.Text.Json;
using Dapper;
using PetFamily.Application.Abstractions;
using PetFamily.Application.Database;
using PetFamily.Application.Dto.Pet;
using PetFamily.Application.Dto.Shared;
using PetFamily.Application.Models;

namespace PetFamily.Application.Pets.Queries.GetFilteredPetsWithPagination;

public class GetFilteredPetsWithPaginationHandlerDapper : IQueryHandler<
    PagedList<PetDto>,
    GetFilteredPetsWithPaginationQuery>
{
    private readonly ISqlConnectionFactory _sqlConnectionFactory;

    public GetFilteredPetsWithPaginationHandlerDapper(ISqlConnectionFactory sqlConnectionFactory)
    {
        _sqlConnectionFactory = sqlConnectionFactory;
    }

    public async Task<PagedList<PetDto>> HandleAsync(
        GetFilteredPetsWithPaginationQuery query,
        CancellationToken cancellationToken)
    {
        var connection = _sqlConnectionFactory.Create();

        var parameters = new DynamicParameters();
        parameters.Add("@PageSize", query.PageSize);
        parameters.Add("@Offset", (query.Page - 1) * query.PageSize);

        /*var sql = """
                  SELECT id, name, position, transfer_details, photos FROM pets
                  ORDER BY position LIMIT @PageSize OFFSET @Offset
                  """;

        //var pets = await connection.QueryAsync<PetDto>(sql, parameters);

        var pets = await connection.QueryAsync<PetDto, string, string, PetDto>(
            sql,
            (pet, jsonsTransferDetails, jsonPhotos) =>
            {
                var tDtos = JsonSerializer.Deserialize<TransferDetailDto[]>(jsonsTransferDetails) ?? [];
                pet.TransferDetails = tDtos;

                var pDtos = JsonSerializer.Deserialize<PhotoDto[]>(jsonPhotos) ?? [];
                pet.Photos = pDtos;
                return pet;
            },
            splitOn: "transfer_details",
        param: parameters);*/
        
        var sql = """
                  SELECT *
                  FROM pets
                  ORDER BY position 
                  LIMIT @PageSize OFFSET @Offset
                  """;

        var result = await connection.QueryAsync<dynamic>(sql, parameters);

        var pets = result.Select(row =>
        {
            var pet = new PetDto
            {
                Id = row.id,
                Name = row.name,
                Description = row.description,
                SpeciesId = row.species_id,
                BreedId = row.breed_id,
                Color = row.color,
                HealthInfo = row.health_info,
                City = row.city,
                House = row.house,
                Apartment = row.apartment,
                Weight = row.weight_info,
                Height = row.height_info,
                OwnerPhoneNumber = row.owner_phone.ToString(),
                IsCastrated = row.is_castrated,
                DateOfBirth = row.date_of_birth,
                IsVaccinated = row.is_vaccinated,
                HelpStatus = row.help_status.ToString(),
                CreationDate = row.creation_date,
                Position = row.position,
                TransferDetails = JsonSerializer.Deserialize<TransferDetailDto[]>(row.transfer_details),
                Photos = JsonSerializer.Deserialize<PhotoDto[]>(row.photos)
            };
            return pet;
        }).ToList();

        var totalCount = await connection.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM pets");

        return new PagedList<PetDto>()
        {
            Items = pets.ToList(),
            PageSize = query.PageSize,
            TotalCount = totalCount,
            Page = query.PageSize
        };
    }
}