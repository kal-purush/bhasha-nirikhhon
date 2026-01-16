using MeowLib.Domain.Book.Exceptions;
using MeowLib.Domain.Character.Entity;
using MeowLib.Domain.Character.Enums;
using MeowLib.Domain.Character.Exceptions;
using MeowLib.Domain.Shared.Result;

namespace MeowLib.Domain.Character.Services;

public interface IBookCharacterService
{
    /// <summary>
    /// Метод прикрепляет персонажа к книге.
    /// </summary>
    /// <param name="characterId">Id персонажа.</param>
    /// <param name="bookId">Id книги.</param>
    /// <param name="role">Роль в книге.</param>
    /// <returns>Результат добавления</returns>
    /// <exception cref="CharacterNotFoundException">Возникает в случае, если персонаж не был найден.</exception>
    /// <exception cref="BookNotFoundException">Возникает в случае, если книга не найдена.</exception>
    /// <exception cref="CharacterAlreadyAttachedToBookException">Возникает в случае, если персонаж уже прикреплён.</exception>
    Task<Result<BookCharacterEntityModel>> AttachCharacterToBookAsync(int characterId, int bookId,
        BookCharacterRoleEnum role);

    /// <summary>
    /// Метод обновляет роль персонажа в книге.
    /// </summary>
    /// <param name="characterId">Id персонажа</param>
    /// <param name="bookId">Id книги.</param>
    /// <param name="newRole">Новая роль.</param>
    /// <returns>Результат обновления.</returns>
    /// <exception cref="BookCharacterNotFoundException">Возникает в случае, если прикреплёный персонаж не был найден.</exception>
    Task<Result> UpdateBookCharacterRoleAsync(int characterId, int bookId, BookCharacterRoleEnum newRole);

    /// <summary>
    /// Метод проверяет прикреплён ли персонаж к книге.
    /// </summary>
    /// <param name="characterId">Id персонажа.</param>
    /// <param name="bookId">Id книги.</param>
    /// <returns>True - если прикреплён, иначе - false</returns>
    Task<bool> CheckCharacterAlreadyAttachedAsync(int characterId, int bookId);
}