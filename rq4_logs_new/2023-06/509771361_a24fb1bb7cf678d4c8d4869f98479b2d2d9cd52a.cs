using AutoMapper;
using GymBackend.Infrastructure.Abstractions.Interfaces;
using GymBackend.UseCases.Common.BaseHandlers;
using GymBackend.UseCases.Common.Dtos.Workout;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace GymBackend.UseCases.Workout.AddWorkouts;

/// <summary>
/// Handler for <see cref="AddWorkoutsCommand"/>.
/// </summary>
public class AddWorkoutsCommandHandler : BaseCommandHandler, IRequestHandler<AddWorkoutsCommand>
{
    private readonly ILoggedUserAccessor loggedUserAccessor;

    /// <summary>
    /// Constructor.
    /// </summary>
    public AddWorkoutsCommandHandler(IMapper mapper, IAppDbContext dbContext, ILoggedUserAccessor loggedUserAccessor) : base(mapper, dbContext)
    {
        this.loggedUserAccessor = loggedUserAccessor;
    }

    /// <inheritdoc />
    public async Task Handle(AddWorkoutsCommand request, CancellationToken cancellationToken)
    {
        var userId = loggedUserAccessor.GetCurrentUserId();
        var workouts = request.Workouts.ToList();

        foreach (var workout in workouts)
        {
            await HandleWorkoutAsync(userId, workout, CancellationToken.None);
        }

        await DbContext.SaveChangesAsync(cancellationToken);
    }

    private async Task HandleWorkoutAsync(Guid userId, WorkoutDto workoutDto, CancellationToken cancellationToken)
    {
        var existWorkout = await DbContext.Workouts.FirstOrDefaultAsync(workout => workout.Id == workoutDto.Id, cancellationToken);
        if (existWorkout == null)
        {
            var domainWorkout = Mapper.Map<Domain.Workouts.Workout>(workoutDto);
            domainWorkout.CreatedById = userId;
            DbContext.Workouts.Add(domainWorkout);
            return;
        }

        if (existWorkout.UpdatedAt < workoutDto.UpdatedAt)
        {
            Mapper.Map(workoutDto, existWorkout);
        }
    }
}