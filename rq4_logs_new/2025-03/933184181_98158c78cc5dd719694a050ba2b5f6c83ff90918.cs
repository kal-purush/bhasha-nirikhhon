using Carter;
using MainService.Domain.Entities;
using MainService.Domain.UseCases;
using MainService.Presentation.DTOs;
using Serilog;
namespace MainService.Presentation.Modules
{
    public class UserModule : ICarterModule
    {
        public void AddRoutes(IEndpointRouteBuilder app)
        {
            var userRoutes = app.MapGroup("api/v1/user");

            userRoutes.MapPost("/create", HandleCreateUser);
            userRoutes.MapGet("/", HandleGetAllUsers);
            userRoutes.MapGet("/{id:int}", HandleGetUserById);
        }

        private static async Task<IResult> HandleCreateUser(
            UserCreateRequest userRequest,
            UserUseCase userUseCase)
        {   
            Log.Information("Start of the controller");
            // Basic validation
            if (string.IsNullOrWhiteSpace(userRequest.Username) ||
                string.IsNullOrWhiteSpace(userRequest.Email) ||
                string.IsNullOrWhiteSpace(userRequest.Password))
            {
                return Results.BadRequest("All fields are required.");
            }

            var newUser = new UserDomain
            {
                Username = userRequest.Username,
                Email = userRequest.Email,
                Password = userRequest.Password,
                CreatedAt = DateTime.UtcNow
            };

            var createdUser = await userUseCase.CreateUser(newUser);
            return Results.Ok(new { Message = "User created successfully", User = createdUser });
        }

        private static IResult HandleGetAllUsers()
        {
            // Placeholder for getting all users
            return Results.Ok("Get all users");
        }

        private static IResult HandleGetUserById(int id)
        {
            // Placeholder for getting a user by ID
            return Results.Ok($"Get user with ID {id}");
        }
    }
}