using AngleSharp.Dom;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Web.Html.Parser.App.Data;

public interface IParserRepository
{
    Task<Guid> AddUpdateGame(int gameWebId, string gameName);
    Task<Guid> AddUpdateGameItem(Guid gameId, string gameItemTitle);
    Task<Guid> AddUpdateUser(int userWebId, string userName);
    Task<Guid> AddUpdateItem(Guid userId, Guid gameId, string description);
    Task<Guid> AddUpdateItemPrice(Guid itemId, double price, int count);
}

public class ParserRepository : IParserRepository
{
    public async Task<Guid> AddUpdateGame(int gameWebId, string gameName)
    {
        return Guid.Empty;
    }

    public async Task<Guid> AddUpdateGameItem(Guid gameId, string gameItemTitle)
    {
        return Guid.Empty;
    }

    public async Task<Guid> AddUpdateUser(int userWebId, string userName)
    {
        return Guid.Empty;
    }

    public async Task<Guid> AddUpdateItem(Guid userId, Guid gameId, string description)
    {
        return Guid.Empty;
    }

    public async Task<Guid> AddUpdateItemPrice(Guid itemId, double price, int count)
    {
        return Guid.Empty;
    }
}