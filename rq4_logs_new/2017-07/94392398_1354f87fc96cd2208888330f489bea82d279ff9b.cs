using Microsoft.EntityFrameworkCore.Scaffolding.Configuration.Internal;
using Microsoft.EntityFrameworkCore.Scaffolding.Internal;

namespace Scaffolding.CustomScaffolding
{
    /// <summary>
    /// DbContextWriter writes out the C# code for the context.
    /// We override this so that we can pluralize the DbSet names.
    /// </summary>
    public class SingularDbContextWriter : DbContextWriter
    {
        public SingularDbContextWriter(
            ScaffoldingUtilities scaffoldingUtilities,
            CSharpUtilities cSharpUtilities)
            : base(scaffoldingUtilities, cSharpUtilities)
        { }

        public override string WriteCode(ModelConfiguration modelConfiguration)
        {
            // There is no good way to override the DbSet naming, as it uses 
            // an internal StringBuilder. This means we can't override 
            // AddDbSetProperties without re-implementing the entire class.
            // Therefore, we have to get the code and then do string manipulation 
            // to replace the DbSet property code

            var code = base.WriteCode(modelConfiguration);

            foreach (var entityConfig in modelConfiguration.EntityConfigurations)
            {
                var entityName = entityConfig.EntityType.Name;
                var setName = Inflector.Inflector.Pluralize(entityName) ?? entityName;

                code = code.Replace(
                    $"DbSet<{entityName}> {entityName}",
                    $"DbSet<{entityName}> {setName}");
            }

            return code;
        }
    }
}