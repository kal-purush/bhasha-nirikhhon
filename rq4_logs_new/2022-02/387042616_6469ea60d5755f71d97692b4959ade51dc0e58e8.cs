namespace Vulder.Admin.Core;

public static class Constants
{
    public static bool RegisterOnlyOneAccount()
    {
        return bool.Parse(Environment.GetEnvironmentVariable("REGISTER_ONLY_ONE_ACCOUNT") ?? "true");
    }
}