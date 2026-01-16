using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GameOnApplication.Models;

namespace GameOnApplication.Interface
{
    /// <summary>
    /// public interface declarations
    /// </summary>
    public interface ILogin
    {
        /// <summary>
        /// user login function declaration
        /// </summary>
        /// <param name="userLoginCredentials"></param>
        /// <returns></returns>
        LoginModel UserLogin(LoginModel userLoginCredentials);

        /// <summary>
        /// user login that returns user logged in information
        /// </summary>
        /// <param name="userLoginCredentials"></param>
        /// <returns></returns>
        UserModel UserLogin(LoginModel userLoginCredentials);

        /// <summary>
        /// user registering for a user account
        /// </summary>
        /// <param name="userRegisterInfo"></param>
        /// <returns></returns>
        UserModel UserRegister(RegisterModel userRegisterInfo);

    }
}