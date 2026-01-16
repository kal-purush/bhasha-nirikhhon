using System.Diagnostics;
using MeowLib.DAL;
using MeowLib.Domain.Character.Dto;
using MeowLib.Domain.Character.Entity;
using MeowLib.Domain.Character.Exceptions;
using MeowLib.Domain.Character.Services;
using MeowLib.Domain.Shared.Exceptions;
using MeowLib.Domain.Shared.Models;
using MeowLib.Domain.Shared.Result;
using Microsoft.EntityFrameworkCore;

namespace MeowLib.Services.Implementation.Production;

public class CharacterService(ApplicationDbContext dbContext) : ICharacterService
{
    public async Task<Result<CharacterEntityModel>> CreateCharacterAsync(CharacterDto character)
    {
        var errors = await ValidateCharacterAsync(character);
        if (errors.Any())
        {
            return Result<CharacterEntityModel>.Fail(new ValidationException(errors));
        }

        // safety: image check in CreateCharacterAsync 
        var foundedImage = await dbContext.Files
            .FirstOrDefaultAsync(f => f.Id == character.Image.Id) ?? throw new UnreachableException();


        var entry = await dbContext.Characters.AddAsync(new CharacterEntityModel
        {
            Name = character.Name,
            Description = character.Description,
            Image = foundedImage
        });

        await dbContext.SaveChangesAsync();
        return Result<CharacterEntityModel>.Ok(entry.Entity);
    }

    public async Task<CharacterEntityModel?> GetCharacterByIdAsync(int characterId)
    {
        return await dbContext.Characters.FirstOrDefaultAsync(c => c.Id == characterId);
    }

    public async Task<Result> DeleteCharacterByIdAsync(int characterId)
    {
        var foundedCharacter = await GetCharacterByIdAsync(characterId);
        if (foundedCharacter is null)
        {
            return Result.Fail(new CharacterNotFoundException());
        }

        dbContext.Characters.Remove(foundedCharacter);
        await dbContext.SaveChangesAsync();

        return Result.Ok();
    }

    public async Task<Result<CharacterEntityModel>> UpdateCharacterAsync(CharacterDto character)
    {
        var errors = await ValidateCharacterAsync(character);
        if (errors.Any())
        {
            return Result<CharacterEntityModel>.Fail(new ValidationException(errors));
        }

        var foundedCharacter = await GetCharacterByIdAsync(character.Id);
        if (foundedCharacter is null)
        {
            return Result<CharacterEntityModel>.Fail(new CharacterNotFoundException());
        }

        // safety: image check in CreateCharacterAsync 
        var foundedImage = await dbContext.Files
            .FirstOrDefaultAsync(f => f.Id == character.Image.Id) ?? throw new UnreachableException();

        foundedCharacter.Name = character.Name;
        foundedCharacter.Description = character.Description;
        foundedCharacter.Image = foundedImage;

        dbContext.Characters.Update(foundedCharacter);
        await dbContext.SaveChangesAsync();

        return Result<CharacterEntityModel>.Ok(foundedCharacter);
    }

    private async Task<List<ValidationErrorModel>> ValidateCharacterAsync(CharacterDto character)
    {
        var errors = new List<ValidationErrorModel>();

        if (character.Name.Length > 120)
        {
            errors.Add(new ValidationErrorModel
            {
                PropertyName = nameof(character.Name),
                Message = "Имя персонажа не может быть больше 120 символов"
            });
        }

        if (character.Description.Length > 1000)
        {
            errors.Add(new ValidationErrorModel
            {
                PropertyName = nameof(character.Description),
                Message = "Описание персонажа не может быть больше 1000 символов"
            });
        }

        if (!await dbContext.Files.AnyAsync(f => f.Id == character.Image.Id))
        {
            errors.Add(new ValidationErrorModel
            {
                PropertyName = nameof(character.Image),
                Message = "Изображение не найдено"
            });
        }

        return errors;
    }
}