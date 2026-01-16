namespace PaderbornUniversity.SILab.Hip.UserStore.Model.Rest
{
    /// <summary>
    /// Represents the user arguments that are handed from the Auth0 Post-User-Registration hook to UserStore.
    /// This class is modeled after the Auth0 Hooks API. See also:
    /// https://auth0.com/docs/hooks/extensibility-points/post-user-registration#starter-code-and-parameters.
    /// </summary>
    public class UserArgsAuth0
    {
        public string Tenant { get; set; }

        public string Username { get; set; }

        public string Email { get; set; }

        public bool EmailVerified { get; set; }
    }
}