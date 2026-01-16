using System;
using MAL_Reviewer_Core.interfaces;
using MAL_Reviewer_Core.models.UserModels;

namespace MAL_Reviewer_Core.controllers
{
    [Serializable]
    /// <summary>
    /// The controller of the user.
    /// </summary>
    public class UserController : ISettings
    {
        #region Properties

        /// <summary>
        /// The default MAL user.
        /// </summary>
        private UserModel DefaultUser { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Parameterless constructor.
        /// </summary>
        public UserController() { }

        #endregion

        #region Public methods       

        /// <summary>
        /// Initializes the current user.
        /// </summary>
        public void Init()
        {
            Core.User = DefaultUser;
        }

        /// <summary>
        /// Resets the user's settings.
        /// </summary>
        public void ResetSettings()
        {
            SeedSettings();
        }

        /// <summary>
        /// Generate default values for the user's settings.
        /// </summary>
        public void SeedSettings()
        {
            DefaultUser = null;
        }

        #endregion
    }
}