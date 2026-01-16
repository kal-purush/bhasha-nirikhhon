
using System.Reflection;
#if NETCOREAPP
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc.ApplicationParts;
using Microsoft.AspNetCore.Mvc.Controllers;
using Nexus.Link.Libraries.Core.Logging;

namespace Nexus.Link.Libraries.Web.AspNet.Startup
{
    // https://snede.net/re-use-controllers-views-and-tag-helpers-in-asp-net-core/
    /// <summary>
    /// Removed controllers that are redundant
    /// </summary>
    internal class RemoveRedundantControllers : 
        IApplicationFeatureProvider<ControllerFeature>
    {
        private readonly ICollection<Type> _controllersToKeep;

        public RemoveRedundantControllers(ICollection<Type> controllersToKeep)
        {
            _controllersToKeep = controllersToKeep;
        }

        public void PopulateFeature(IEnumerable<ApplicationPart> parts, ControllerFeature 
            feature)
        {
            var serviceAssembly = Assembly.GetEntryAssembly();
            var redundantControllers = feature.Controllers
                .Where(ti => ti.Assembly != serviceAssembly) // Keep all controllers from the main assembly
                .Where(ti => !_controllersToKeep.Contains(ti.AsType()))
                .ToList();

            foreach (var redundantController in redundantControllers)
            {
                feature.Controllers.Remove(redundantController);
                Log.LogVerbose($"Removed controller {redundantController.FullName}");
            }
        }
    }
}
#endif