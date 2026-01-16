using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.AspNetCore.Mvc.Controllers;
using QuestStore.API.Controllers;
using QuestStore.Core;
using QuestStore.Core.Entities;

namespace QuestStore.API.GenericControllersFactory
{
    public class GenericControllersFeatureProvider : IApplicationFeatureProvider<ControllerFeature>
    {
        public void PopulateFeature(IEnumerable<ApplicationPart> parts, ControllerFeature feature)
        {
            // types of entities which inherit from the BaseEntity and have the GenerateController attribute.
            var entitiesTypes = Assembly.GetAssembly(typeof(BaseEntity))
                ?.ExportedTypes.Where(
                    t => !t.IsAbstract && t.IsSubclassOf(typeof(BaseEntity)) && t.GetCustomAttribute<GenerateControllerAttribute>() != null);
            if (entitiesTypes == null) return;

            foreach (var type in entitiesTypes)
            {
                feature.Controllers.Add(typeof(GenericController<>).MakeGenericType(type).GetTypeInfo());
            }
        }
    }
}