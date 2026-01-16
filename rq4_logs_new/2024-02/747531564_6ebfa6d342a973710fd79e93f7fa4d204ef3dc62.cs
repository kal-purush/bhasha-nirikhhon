using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Online_Survey.Areas.Identity.Data;

namespace Online_Survey.Data;

public class Online_SurveyContext : IdentityDbContext<Online_SurveyUser>
{
    public Online_SurveyContext(DbContextOptions<Online_SurveyContext> options)
        : base(options)
    {
    }

    protected Online_SurveyContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);
        // Customize the ASP.NET Identity model and override the defaults if needed.
        // For example, you can rename the ASP.NET Identity table names and more.
        // Add your customizations after calling base.OnModelCreating(builder);
    }
}