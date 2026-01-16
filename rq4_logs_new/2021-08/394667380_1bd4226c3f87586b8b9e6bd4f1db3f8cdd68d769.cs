using Microsoft.Dynamics.BusinessConnectorNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AxNetConnectorToIDM
{
    public class AxConnector
    {
        private Axapta ax { get; set; }
        public void Connect()
        {
            try
            {
                ax = new Axapta();
                ax.Logon(null, null, null, null);

            }
            catch (Exception e)
            {
                System.Console.WriteLine("An error occurred in object creation or Axapta logon: {0}", e.Message);
            }
        }

        public void LogOff()
        {
            ax.Logoff();
            ax.Dispose();
        }

        /// <summary>
        /// Get User
        /// <returns>
        /// Container:
        /// 1: SID - String
        /// 2: Domain User Name - String
        /// 3: Name - String
        /// 4: Enable - Boolean
        /// 5: Axapta Id - String
        /// 6: Email - String
        /// 7: Company - String
        /// </returns>
        /// </summary>
        public AxaptaContainer GetUsers()
        {
            try
            {
                return (AxaptaContainer)ax.CallStaticClassMethod("UKT_UserManagement", "GetUsers");

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
            }

            return null;
        }


        /// <summary>
        /// Get Group
        /// <returns>
        /// Container:
        /// 1: Id - String
        /// 2: Name - String
        /// </returns>
        /// </summary>
        public AxaptaContainer GetGroups()
        {
            try
            {
                return (AxaptaContainer)ax.CallStaticClassMethod("UKT_UserManagement", "GetGroups");

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
            }

            return null;
        }

        /// <summary>
        /// Get Group Members
        /// <returns>
        /// Container:
        /// 1: Group Id - String
        /// 2: Axapta User Id - String
        /// </returns>
        /// </summary>
        public AxaptaContainer GetGroupMembers()
        {
            try
            {
                return (AxaptaContainer)ax.CallStaticClassMethod("UKT_UserManagement", "GetGroupMembers");

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
            }

            return null;
        }


        /// <summary>
        /// Create a new user account
        /// <returns>
        /// Axapta Id:
        /// </returns>
        /// </summary>
        public String CreateUser(NewUser newUser)
        {
            try
            {
                return (String)ax.CallStaticClassMethod("UKT_UserManagement", "CreateUser", newUser.Domain, newUser.UserAccount, newUser.CompanyId);

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
                return "ERROR";
            }
        }

        /// <summary>
        /// Change user status
        /// <returns>
        /// Status: Success or Error
        /// </returns>
        /// </summary>
        public String ChangeUserStatus(String domain, String userAccount, bool enable)
        {
            try
            {
                return (String)ax.CallStaticClassMethod("UKT_UserManagement", "ChangeUserStatus", domain, userAccount, enable);

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
                return "ERROR";
            }
        }

        /// <summary>
        /// Update user
        /// <returns>
        /// Status: Success or Error
        /// </returns>
        /// </summary>
        public String UpdateUser(String domain, String userAccount, String name, String email)
        {
            try
            {
                return (String)ax.CallStaticClassMethod("UKT_UserManagement", "UpdateUser", domain, userAccount, name, email);

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
                return "ERROR";
            }
        }

        /// <summary>
        /// Add user to group
        /// <returns>
        /// Status: Success or Error
        /// </returns>
        /// </summary>
        public String AddUserToGroup(String domain, String userAccount, String groupId)
        {
            try
            {
                return (String)ax.CallStaticClassMethod("UKT_UserManagement", "AddUserToGroup", domain, userAccount, groupId);

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
                return "ERROR";
            }
        }

        /// <summary>
        /// Remove user from group
        /// <returns>
        /// Status: Success or Error
        /// </returns>
        /// </summary>
        public String RemoveUserFromGroup(String domain, String userAccount, String groupId)
        {
            try
            {
                return (String)ax.CallStaticClassMethod("UKT_UserManagement", "RemoveUserFromGroup", domain, userAccount, groupId);

            }
            catch (Exception e)
            {
                this.LogOff();
                System.Console.WriteLine("An error has been encountered during CallStaticClassMethod: {0}", e.Message);
                return "ERROR";
            }
        }
    }
}