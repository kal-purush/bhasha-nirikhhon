namespace GradeBook.BusinessLogic.Constants
{
    public static class Constants
    {
        public static class ExceptionMessages
        {
            public static class Admin
            {
                public const string UniqueRoleException = "User cannot be pupil and teacher simultaneously or user already have this role";
            }
            public static class Auth
            {
                public const string UserAlreadyCreatedException = "User is already created";
                public const string IncorrectDataException = "Your email or password is incorrect";
                public const string GeneratedKeyException = "Key cannot be generated";
            }
            public static class Global
            {
                public const string NotFoundException = "Entity does not found";
                public const string UserNotFoundException = "User cannot be found";
            }
            public static class Grade
            {
                public const string IncorrectRoleException = "Only pupil can have grades";
                public const string IncorrectPupilException = "Pupil is not member of this class";
                public const string IllegalAccessException = "User doesn\'t have access to this information";
            }
            public static class Pupil
            {
                public const string IncorrectRoleException = "User is not a pupil";
            }
        }
    }
}