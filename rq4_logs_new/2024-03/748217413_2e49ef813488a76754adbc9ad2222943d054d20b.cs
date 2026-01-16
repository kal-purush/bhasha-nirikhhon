using Map.Domain.Entities;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;

namespace Map.EFCore.Data;

public class DBInitializer
{
    private readonly MapContext _context;
    private readonly UserManager<MapUser> _userManager;
    private readonly RoleManager<IdentityRole<Guid>> _roleManager;

    /// <summary>
    /// Initializes a new instance of the <see cref="DBInitializer"/> class.
    /// </summary>
    /// <param name="context">The context.</param>
    /// <param name="userManager">The user manager.</param>
    /// <param name="roleManager">The role manager.</param>
    public DBInitializer(MapContext context, UserManager<MapUser> userManager, RoleManager<IdentityRole<Guid>> roleManager)
    {
        _context = context;
        _userManager = userManager;
        _roleManager = roleManager;
    }

    /// <summary>
    /// Initializes the.
    /// </summary>
    /// <returns>A Task.</returns>
    public async Task<bool> Initialize(bool force)
    {
        _context.Database.EnsureCreated();

        if (!force && _context.Roles.Any() && _context.Users.Any())
            return false;

        await _context.TravelRoad.ExecuteDeleteAsync();
        await _context.Travel.ExecuteDeleteAsync();
        await _context.Trip.ExecuteDeleteAsync();
        await _context.MapUser.ExecuteDeleteAsync();
        await _context.Users.ExecuteDeleteAsync();
        await _context.Roles.ExecuteDeleteAsync();

        #region AddRoles

        string[] roles = Roles.GetAllRoles();

        foreach (string role in roles)
        {
            if (!await _roleManager.RoleExistsAsync(role))
            {
                IdentityResult resultAddRole = await _roleManager.CreateAsync(new IdentityRole<Guid>(role));
                if (!resultAddRole.Succeeded)
                    throw new ApplicationException("Adding role '" + role + "' failed with error(s): " + resultAddRole.Errors);
            }
        }

        await _context.SaveChangesAsync();

        #endregion

        string pwd = "NMdRx$HqyT8jX6";
        #region AddAdmin

        MapUser userAdmin = new()
        {
            UserName = "Dercraker",
            Email = "dercraker@from-a2b.fr",
            EmailConfirmed = true,
            PhoneNumber = "0606060606",
            PhoneNumberConfirmed = true,
        };


        IdentityResult? resultAddUser = await _userManager.CreateAsync(userAdmin, pwd);
        if (!resultAddUser.Succeeded)
            throw new ApplicationException("Adding user '" + userAdmin.UserName + "' failed with error(s): " + resultAddUser.Errors);

        IdentityResult resultAddRoleToUser = await _userManager.AddToRoleAsync(userAdmin, Roles.Admin);
        if (!resultAddRoleToUser.Succeeded)
            throw new ApplicationException("Adding user '" + userAdmin.UserName + "' to role '" + Roles.Admin + "' failed with error(s): " + resultAddRoleToUser.Errors);

        resultAddRoleToUser = await _userManager.AddToRoleAsync(userAdmin, Roles.User);
        if (!resultAddRoleToUser.Succeeded)
            throw new ApplicationException("Adding user '" + userAdmin.UserName + "' to role '" + Roles.User + "' failed with error(s): " + resultAddRoleToUser.Errors);

        await _context.SaveChangesAsync();

        #endregion

        #region addSingleUser
        MapUser simpleUser = new()
        {
            UserName = "Dercraker2",
            Email = "dercraker2@from-a2b.fr",
            EmailConfirmed = true,
            PhoneNumber = "(600) 890-5820",
            PhoneNumberConfirmed = true,
        };


        resultAddUser = await _userManager.CreateAsync(simpleUser, pwd);
        if (!resultAddUser.Succeeded)
            throw new ApplicationException("Adding user '" + simpleUser.UserName + "' failed with error(s): " + resultAddUser.Errors);

        resultAddRoleToUser = await _userManager.AddToRoleAsync(simpleUser, Roles.User);
        if (!resultAddRoleToUser.Succeeded)
            throw new ApplicationException("Adding user '" + simpleUser.UserName + "' to role '" + Roles.User + "' failed with error(s): " + resultAddRoleToUser.Errors);

        await _context.SaveChangesAsync();
        #endregion

        #region AddUser
        List<MapUser> mapUsers = new List<MapUser>
        {
            new MapUser
            {
                UserName = "MikeStewart",
                Email = "go@an.fj",
                EmailConfirmed = true,
                PhoneNumber = "(859) 897-8305",
                PhoneNumberConfirmed = true,
            },
            new MapUser
            {
                UserName = "RosaBell",
                Email = "tew@ron.mr",
                EmailConfirmed = true,
                PhoneNumber = "(901) 951-7529",
                PhoneNumberConfirmed = true,
            },
            new MapUser
            {
                UserName = "PaulineGarza",
                Email = "sokinva@pugbe.np",
                EmailConfirmed = true,
                PhoneNumber = "(201) 658-4063",
                PhoneNumberConfirmed = true,
            },
            new MapUser
            {
                UserName = "MikeFernandez",
                Email = "na@zal.ck",
                EmailConfirmed = true,
                PhoneNumber = "(414) 721-6737",
                PhoneNumberConfirmed = true,
            },
            new MapUser
            {
                UserName = "HallieDawson",
                Email = "zocinbez@paegwe.bh",
                EmailConfirmed = true,
                PhoneNumber = "(767) 688-2269",
                PhoneNumberConfirmed = true,
            },
        };

        Random random = new Random();
        foreach (MapUser user in mapUsers)
        {
            user.EmailConfirmed = random.Next(2) == 0;
            user.PhoneNumberConfirmed = random.Next(2) == 0;

            resultAddUser = await _userManager.CreateAsync(user, pwd);
            if (!resultAddUser.Succeeded)
                throw new ApplicationException("Adding user '" + user.UserName + "' failed with error(s): " + resultAddUser.Errors);

            resultAddRoleToUser = await _userManager.AddToRoleAsync(user, Roles.User);
            if (!resultAddRoleToUser.Succeeded)
                throw new ApplicationException("Adding user '" + user.UserName + "' to role '" + Roles.User + "' failed with error(s): " + resultAddRoleToUser.Errors);
            await _context.SaveChangesAsync();
        }
        #endregion

        #region AddTrips
        foreach (MapUser user in _context.MapUser.ToList())
        {
            int numTrips = random.Next(2, 11);
            for (int i = 0; i < numTrips; i++)
            {
                Trip trip = new Trip
                {
                    User = user,
                    Name = $"Trip {i + 1} for {user.UserName}",
                    Description = $"Description for Trip {i + 1} for {user.UserName}",
                    StartDate = new DateOnly(random.Next(2022, 2025), random.Next(1, 13), random.Next(1, 29)),
                    EndDate = new DateOnly(random.Next(2022, 2025), random.Next(1, 13), random.Next(1, 29)),
                    BackgroundPicturePath = "https://via.placeholder.com/150"
                };

                await _context.Trip.AddAsync(trip);

                int numSteps = random.Next(2, 6);
                List<Step> steps = new List<Step>();

                for (int j = 0; j < numSteps; j++)
                {
                    Step step = new Step
                    {
                        TripId = trip.TripId,
                        Order = j + 1,
                        Name = $"Step {j + 1} for Trip {i + 1} of user {user.UserName}",
                        Longitude = (decimal)(random.NextDouble() * 360 - 180),
                        Latitude = (decimal)(random.NextDouble() * 180 - 90),
                        Description = $"Description for Step {j + 1} for Trip {i + 1}",
                        StartDate = DateTime.Now.AddDays(j),
                        EndDate = DateTime.Now.AddDays(j + 1)
                    };

                    steps.Add(step);

                    trip.Steps = steps;
                }
                await _context.SaveChangesAsync();

                for (int k = 0; k < steps.Count - 1; k++)
                {
                    Travel travel = new Travel
                    {
                        OriginStep = steps[k],
                        DestinationStep = steps[k + 1],
                        TransportMode = "Mode of Transport",
                        Distance = (decimal)random.NextDouble() * 1000,
                        Duration = (decimal)random.NextDouble() * 24
                    };
                    TravelRoad travelRoad = new TravelRoad
                    {
                        TravelId = travel.TravelId,
                        Travel = travel,
                        RoadCoordinates = "Coordinates of Road"
                    };
                    travel.TravelRoad = travelRoad;
                    steps[k].TravelAfter = travel;

                    await _context.SaveChangesAsync();
                }
            }
        }

        #endregion

        return true;
    }
}
