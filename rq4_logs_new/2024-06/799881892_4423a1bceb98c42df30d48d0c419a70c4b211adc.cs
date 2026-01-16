using Application.Common.Interfaces;

namespace Application.UseCases.Departments.Commands.CreateDepartment
{
    /// <summary>
    /// Request to create a new Department.
    /// </summary>
    public class CreateDepartmentCommand : IRequest<int>
    {
        public required string Name { get; init; }
        public string Description { get; init; } = String.Empty;
        public List<Category>? Categories { get; init; }
        public List<Product>? Products { get; init; }
    }

    /// <summary>
    /// Request handler for creating a new Department.
    /// </summary>
    public class CreateDepartmentCommandHandler : IRequestHandler<CreateDepartmentCommand, int>
    {
        private readonly IApplicationDbContext dbContext;

        public CreateDepartmentCommandHandler(IApplicationDbContext _dbContext)
        {
            dbContext = _dbContext;
        }

        /// <summary>
        /// Creates a new Department and adds it into the database.
        /// </summary>
        public async Task<int> Handle(CreateDepartmentCommand request, CancellationToken cancellationToken)
        {
            var department = new Department
            {
                Name = request.Name,
                Description = request.Description,
                Categories = request.Categories ?? [],
                Products = request.Products ?? [],
            };

            department.AddDomainEvent(new DepartmentCreatedEvent(department));

            dbContext.Departments.Add(department);

            await dbContext.SaveChangesAsync(cancellationToken);

            return department.Id;
        }
    }
}