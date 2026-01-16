namespace ProjectBank.Infrastructure.test;

public class ProjectRepositoryTests
{
    private readonly IProjectBankContext _context;
    
    private readonly IProjectRepository _repo;
    
    //detect redundant calls
    private bool _disposedValue;

    ~ProjectRepositoryTests() => Dispose(false);
    public ProjectRepositoryTests()
    {
        //establish connection
        var connection = new SqliteConnection("Filename=:memory:");
        connection.Open();
        var builder = new DbContextOptionsBuilder<ProjectBankContext>();
        builder.UseSqlite(connection);
        var context = new ProjectBankContext(builder.Options);
        context.Database.EnsureCreated();
        
        //seed some data
        var unknownAccount = new Account("UnknownToken") {Id = 1, AccountType = AccountType.Student};
        var aiKeyword = new Keyword("AI") {Id = 1};
        var machineLearnKey = new Keyword("Machine Learning") {Id = 2};
        var saveListAccount = new Account("AuthorToken") {Id = 3,  AccountType = AccountType.Student};
        var aiProject = new Project("Artificial Intelligence 101")
        { 
            Id = 1, AuthorId = 1,Author = unknownAccount ,Keywords = new[]{aiKeyword, machineLearnKey},
            Ects = 7, Description = "A dummies guide to AI. Make your own AI friend today", Created = DateTime.Now, Accounts = new[] {saveListAccount}
        };
        var mlProject = new Project("Machine Learning for dummies")
        {
            Id = 2, Ects = 15, Description = "Very easy guide just for you", Degree = Degree.PHD, Created = DateTime.Now
            ,Keywords = new[] {machineLearnKey}
             
        };
        context.Projects.AddRange(aiProject, mlProject);
        context.Keywords.Add(new Keyword("Design"){Id = 3});
        context.Accounts.Add( new Account("Token2") { Id = 2 , AccountType = AccountType.Supervisor});
        context.SaveChanges();
        
        //init dbContext and Repo
        _context = context;
        _repo = new ProjectRepository(_context);
    }
    [Fact]
    public async Task ReadAllAsync_Returns_All_Keywords()
    {
        var projects = await _repo.ReadAllAsync();
        Assert.Collection(projects,
            project => Assert.Equal(new ProjectDto(1, "UnknownToken","Artificial Intelligence 101",
                "A dummies guide to AI. Make your own AI friend today"), project),
            project => Assert.Equal(new ProjectDto(2,null, "Machine Learning for dummies",
                "Very easy guide just for you" ), project)
        );
    }
    
    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                // dispose managed state (managed objects)
                _context.Dispose();
            }
            // free unmanaged resources (unmanaged objects) and override finalizer
            // set large fields to null
            _disposedValue = true;
        }
    }

    //From BDSA2021.
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}