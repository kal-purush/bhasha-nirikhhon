namespace jobsearch.Core
{
    public interface IUnitOfWork
    {
        #region repositories
        IJobRepository Jobs { get; }
        #endregion

        Task<int> CommitAsync();
    }
}