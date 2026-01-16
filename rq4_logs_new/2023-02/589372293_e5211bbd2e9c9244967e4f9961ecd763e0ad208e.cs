using DomainDrivenDesign.List5_4.Interfaces;
using LiteDB;
using Microsoft.Extensions.DependencyInjection;

namespace DomainDrivenDesign.List5_4;

public class UserRepository : IUserRepository
{
    #region inner class
    // データーベースむけのクラスに変換するところはイマイチかも
    private record class DataBaseUsersColumn(
        string UserName,
        string UserId);
    #endregion

    #region private field
    private readonly string _path;
    private readonly string _table;
    #endregion

    #region constructor
    public UserRepository(string path, string table)
    {
        _path = path;
        _table = table;
    }
    #endregion

    #region public method
    public bool Exists(IUser user)
    {
        using var litedb = new LiteDatabase(_path);
        var column = litedb.GetCollection<DataBaseUsersColumn>(_table);
        return column.Query().Where(x => x.UserName == user.UserName.Name).FirstOrDefault() is not null;
    }

    public IUser? Find(IUserName userName)
    {
        using var litedb = new LiteDatabase(_path);
        var column = litedb.GetCollection<DataBaseUsersColumn>(_table);
        var found = column.Query().Where(x => x.UserName == userName.Name).FirstOrDefault();
        if (found is null)
            return null;

        return DependencyInjection.Services.GetService<IUserFactory>()?.Create(found.UserName, found.UserId);
    }

    public bool Save(IUser user)
    {
        using var litedb = new LiteDatabase(_path);
        var column = litedb.GetCollection<DataBaseUsersColumn>(_table);
        var data = new DataBaseUsersColumn(user.UserName.Name, user.UserId.Id);
        if (column.Query().Where(x => x.UserName == data.UserName).FirstOrDefault() is not null)
            return false;

        _ = column.Insert(data);
        return true;
    }
    #endregion
}