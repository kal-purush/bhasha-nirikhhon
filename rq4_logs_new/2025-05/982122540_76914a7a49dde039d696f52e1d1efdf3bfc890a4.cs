namespace Api.Common.Routing
{
    public class CustomerRoutes : BaseRoute
    {
        public const string Prefix = $"{Base}/customer";

        public const string Register = $"{Prefix}/register";
        public const string Login = $"{Prefix}/login";
        public const string Logout = $"{Prefix}/logout";
        public const string Update = $"{Prefix}/{{id:guid}}";
        public const string RefreshToken = $"{Prefix}/refresh-token";
        public const string ConfirmEmail = $"{Prefix}/confirm-email";
        public const string ResendConfirmEmail = $"{Prefix}/resend-confirm-email";
    }
}