namespace CasDotnetSdk.PasswordHashers
{
    public enum PasswordHasherType
    {
        Argon2 = 1,
        BCrypt = 2,
        SCrypt = 3
    }

    public static class PasswordHasherFactory
    {
        public static IPasswordHasherBase Get(PasswordHasherType type)
        {
            IPasswordHasherBase hasher = null;
            switch (type)
            {
                case PasswordHasherType.Argon2:
                    hasher = new Argon2Wrapper();
                    break;
                case PasswordHasherType.BCrypt:
                    hasher = new BcryptWrapper();
                    break;
                case PasswordHasherType.SCrypt:
                    hasher = new SCryptWrapper();
                    break;
            }
            return hasher;
        }
    }
}