using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LucyCover_Database
{
    public interface ITestUserDetails
    {
        public static Guid Id = new Guid("F85FEC77-10A4-4AFA-9714-B8BB6F0C4202");
        public static string Email = "testuser@lucycover.com";
        public static string FirstName = "Test";
        public static string LastName = "User";
        public static string Password = "test";
        public static string AvatarSrc = "";
    }
}