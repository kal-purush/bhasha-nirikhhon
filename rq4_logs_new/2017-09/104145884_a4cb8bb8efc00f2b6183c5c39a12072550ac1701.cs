namespace HardySoft.GpsTracker.ViewModels
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using HardySoft.GpsTracker.Models;
    using Prism.Mvvm;

    /// <summary>
    /// A view model for dashboard page.
    /// </summary>
    public class DashboardViewModel : BindableBase
    {
        /// <summary>
        /// Gets a list of supported activity types.
        /// </summary>
        public IEnumerable<ActivityTypes> SupportedActivityTypes => Enum.GetValues(typeof(ActivityTypes)).Cast<ActivityTypes>();
    }
}