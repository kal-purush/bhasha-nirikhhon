using Answer.King.Domain.Repositories;
using LiteDB;

namespace Answer.King.Infrastructure.Repositories;

public abstract class BaseRepository : IBaseRepository
{
    protected BaseRepository(ILiteDbConnectionFactory connections)
    {
        this.Db = connections.GetConnection();
    }

    protected ILiteDatabase Db { get; }

    public void BeginTransaction()
    {
        this.Db.BeginTrans();
    }

    public void CommitTransaction()
    {
        this.Db.Commit();
    }

    public void RollbackTransaction()
    {
        this.Db.Rollback();
    }
}