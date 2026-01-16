
namespace ProjectBank.Infrastructure.test;

public class AccountRepositoryTests
{
    private readonly IProjectBankContext _context;
    private readonly IAccountRepository _repo;
    public AccountRepositoryTests()
    {
        //establish connection
        var connection = new SqliteConnection("Filename=:memory:");
        connection.Open();
        var builder = new DbContextOptionsBuilder<ProjectBankContext>();
        builder.UseSqlite(connection);
        var context = new ProjectBankContext(builder.Options);
        context.Database.EnsureCreated();
        
        //seed some data
        var unknownAccount = new Account("UnknownToken") {Id = 1};
        var aiKeyword = new Keyword("AI") {Id = 1};
        var machineLearnKey = new Keyword("Machine Learning") {Id = 2};
        var aiProject = new Project("Artificial Intelligence 101"){ Id = 1, AuthorId = 1,Author = unknownAccount ,Keywords = new[]{aiKeyword, machineLearnKey}, Ects = 7, Description = "A dummies guide to AI. Make your own AI friend today"};
        var saveListAccount = new Account("AuthorToken") {Id = 3, SavedProjects = new[] {aiProject}};
        context.Projects.Add(aiProject);
        context.Accounts.AddRange(saveListAccount, new Account("Token2") { Id = 2 });
        context.SaveChanges();
        
        //init dbContext and Repo
        _context = context;
        _repo = new AccountRepository(_context);
    }
    [Fact]
    public void Test1()
    {

    }
}