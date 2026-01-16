namespace AnagramSolver.Contracts.Interfaces
{
    public interface IUserService
    {
        public void CreateUser(string firstName, string lastName, string email, string pass, string favouriteWords);
    }
}