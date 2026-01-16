using FluentValidation;
using FluentValidation.Results;
using api.Data;
using api.Shared;
using api.Users.Models;
using MediatR;
using System.Net;

namespace api.Users.Commands;
public record UpdateUserCommand(string FirstName,
                                string LastName,
                                string Email,
                                Role Role,
                                int UserId,
                                int CreatorId) : IRequest<Response>;

public class UpdateUserCommandValidator : AbstractValidator<UpdateUserCommand>
{
    public UpdateUserCommandValidator()
    {
        RuleFor(x => x.FirstName)
            .NotNull().WithMessage("FirstName can't be null.")
            .NotEmpty().WithMessage("FirstName can't be empty.")
            .Length(0, 255).WithMessage("Name must contain 0 to 255 characters.");

        RuleFor(x => x.LastName)
            .NotNull().WithMessage("LastName can't be null.")
            .NotEmpty().WithMessage("LastName can't be empty.")
            .Length(0, 255).WithMessage("Name must contain 0 to 255 characters.");

        RuleFor(x => x.Email)
        .EmailAddress().WithMessage("E-mail can't be empty.")
        .Length(0, 255).WithMessage("E-mail must contain 0 to 255 characters.");

        RuleFor(x => x.UserId).NotEmpty().WithMessage("UserId can't be empty.");

        RuleFor(x => x.CreatorId).NotEmpty().WithMessage("CreatorId can't be empty.");

        RuleFor(x => x.Role)
        .IsInEnum().WithMessage("Role can't be null.");
    }
}

public class UpdateUserCommandHandler : IRequestHandler<UpdateUserCommand, Response>
{
    private readonly DataContext _context;

    public UpdateUserCommandHandler(DataContext context) => _context = context;

    public async Task<Response> Handle(UpdateUserCommand request, CancellationToken cancellationToken)
    {
        var validationResult = await ValidateRequestAsync(request);
        if (!validationResult.IsValid)
            return new Response("Validation failed", false, validationResult.Errors.Select(x => x.ErrorMessage).ToList());

        var creator = _context.Users.FirstOrDefault(x => x.Id == request.CreatorId);
        if (creator == null)
            return new Response("Creator not found", false, new { HttpStatusCode = HttpStatusCode.NotFound });

        if (creator.Role != Role.Admin)
            return new Response("You don't have permission to update users", false, new { HttpStatusCode = HttpStatusCode.Unauthorized });

        var user = _context.Users.FirstOrDefault(x => x.Id == request.UserId);
        if (user == null)
            return new Response("User not found", false, new { HttpStatusCode = HttpStatusCode.NotFound });

        if (UserExists(request.Email))
            return new Response("User already exists", false, new { HttpStatusCode = HttpStatusCode.BadRequest });

        user.FirstName = request.FirstName;
        user.LastName = request.LastName;
        user.Email = request.Email;
        user.Role = request.Role;

        await _context.SaveChangesAsync();
        return new Response("User updated successfully", true);
    }

    private async Task<ValidationResult> ValidateRequestAsync(UpdateUserCommand request)
    {
        var validation = new UpdateUserCommandValidator();
        return await validation.ValidateAsync(request);
    }

    private bool UserExists(string email)
    {
        return _context.Users.Any(x => x.Email == email);
    }
}
