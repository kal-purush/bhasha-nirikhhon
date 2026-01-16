using FluentValidation;
using FluentValidation.Results;
using Grpc.Core;
using TaskFlow.ProjectService;

public class ProjectController : ProjectService.ProjectServiceBase
{

    private readonly IValidator<CreateProjectReq> _validator;
    public ProjectController(IValidator<CreateProjectReq> validator)
    {
        _validator = validator;
    }

    public override async Task<CreateProjectRes> CreateProject(CreateProjectReq request, ServerCallContext context)
    {
        ValidationResult validationResult = await _validator.ValidateAsync(request);
        if (!validationResult.IsValid)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, string.Join(", ", validationResult.Errors.Select(e => e.ErrorMessage))));
        }
        // Create response data
        var projectResponse = new ProjectRes
        {
            Id = "12345",
            Name = request.Name,
            Description = request.Description,
            OwnerId = request.OwnerId
        };

        return new CreateProjectRes { Data = projectResponse };
    }
}