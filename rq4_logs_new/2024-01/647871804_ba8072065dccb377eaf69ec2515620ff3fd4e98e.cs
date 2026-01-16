using MeowLib.Domain.Character.Dto;
using MeowLib.Domain.Character.Entity;
using MeowLib.Domain.Character.Exceptions;
using MeowLib.Domain.Shared.Exceptions;
using MeowLib.Domain.Shared.Result;

namespace MeowLib.Domain.Character.Services;

public interface ICharacterService
{
    /// <summary>
    /// Метод создаёт нового персонажа.
    /// </summary>
    /// <param name="character">Данные для создания персонажа.</param>
    /// <returns>Результат создания персонажа.</returns>
    /// <exception cref="ValidationException">Возникает в случае ошибки валидации данныех.</exception>
    Task<Result<CharacterEntityModel>> CreateCharacterAsync(CharacterDto character);

    /// <summary>
    /// Метод получает персонажа по Id.
    /// </summary>
    /// <param name="characterId">Id персонажа.</param>
    /// <returns>Найденного персонажа.</returns>
    Task<CharacterEntityModel?> GetCharacterByIdAsync(int characterId);

    /// <summary>
    /// Метод удаляет персонажа по Id.
    /// </summary>
    /// <param name="characterId">Id персонажа.</param>
    /// <returns>Результат удаления.</returns>
    /// <exception cref="CharacterNotFoundException">Возникает в случае, если персонаж не найден.</exception>
    Task<Result> DeleteCharacterByIdAsync(int characterId);

    /// <summary>
    /// Метод обновляет информацию о персонаже.
    /// </summary>
    /// <param name="character">Данные для обновления.</param>
    /// <returns>Результат обновления.</returns>
    /// <exception cref="ValidationException">Возникает в случае ошибки валидации.</exception>
    /// <exception cref="CharacterNotFoundException">Возникает в случае если персонаж не был найден.</exception>
    Task<Result<CharacterEntityModel>> UpdateCharacterAsync(CharacterDto character);
}