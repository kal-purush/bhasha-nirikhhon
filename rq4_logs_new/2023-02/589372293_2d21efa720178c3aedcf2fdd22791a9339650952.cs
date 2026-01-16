using LiteDB;

namespace DomainDrivenDesign.List4_4;

public class UserService
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
    public UserService(string path, string table)
    {
        _path = path;
        _table = table;
    }
    #endregion

    #region public method
    public bool Exists(User user)
    {
        using var litedb = new LiteDatabase(_path);
        var column = litedb.GetCollection<DataBaseUsersColumn>(_table);
        var data = new DataBaseUsersColumn(user.UserName.Value, user.UserId.Value);

        var result = column.Query().Where(x => x.UserName == data.UserName).ToArray();
        return result.Length > 0;
    }

    public bool Insert(User user)
    {
        using var litedb = new LiteDatabase(_path);
        var column = litedb.GetCollection<DataBaseUsersColumn>(_table);
        var data = new DataBaseUsersColumn(user.UserName.Value, user.UserId.Value);

        var result = column.Query().Where(x => x.UserName == data.UserName).ToArray();
        if (result.Length > 0)
            return false;

        _ = column.Insert(data);
        return true;
    }
    #endregion
}