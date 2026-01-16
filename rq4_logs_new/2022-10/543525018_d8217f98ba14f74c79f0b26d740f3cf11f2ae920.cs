using System;
using System.DirectoryServices;
using System.DirectoryServices.AccountManagement;

namespace PasswordExpire
{
    internal delegate void UserDelegate (User user);

    internal class AdService
    {
        private readonly PrincipalContext _context;

        public AdService ( )
        {
            _context = new PrincipalContext(ContextType.Domain);
        }


        public void Run (string groupName, UserDelegate method)
        {
            var group = GetGroup(groupName);

            foreach (var groupMember in group.Members)
            {
                var user = GetUser(groupMember.SamAccountName);
                if (method != null) method.Invoke(user);
            }
        }

        private GroupPrincipal GetGroup (string groupName)
        {

            using (var userPrinciple = new GroupPrincipal(_context))
            {
                userPrinciple.SamAccountName = groupName;

                using (var search = new PrincipalSearcher(userPrinciple))
                {
                    GroupPrincipal group = (GroupPrincipal)search.FindOne( );

                    if (group == null)
                    {
                        throw new Exception("not found directory");
                    }
                    return group;
                }
            }

        }

        private User GetUser (string userName)
        {
            using (var userPrinciple = new UserPrincipal(_context))
            {
                userPrinciple.SamAccountName = userName;

                using (var search = new PrincipalSearcher(userPrinciple))
                {
                    UserPrincipal user = (UserPrincipal)search.FindOne( );

                    if (user == null)
                    {
                        throw new Exception("user authenticated but not found in directory");
                    }

                    return new User
                    {
                        guid = user.Guid,
                        FirstName = user.GivenName,
                        LastName = user.Surname,
                        Mobile = GetUserMobile(user),
                        Enabled = user.Enabled ?? false,
                        LastPasswordSet = user.LastPasswordSet,
                        LastLogon = user.LastLogon,
                        PasswordNewerExpire = user.PasswordNeverExpires,
                    };
                }
            }
        }

        private string GetUserMobile (UserPrincipal user)
        {
            using (DirectoryEntry de = user.GetUnderlyingObject( ) as DirectoryEntry)
            {
                if (de != null)
                {
                    return de.Properties["mobile"].Value as string;
                }
                return "";
            }
        }
    }
}