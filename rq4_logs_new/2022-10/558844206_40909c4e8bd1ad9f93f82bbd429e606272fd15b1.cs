using LoginAuthMicroServices.Core.Utils.Results;
using LoginAuthMicroServices.Core.Utils.Security.Jwt;
using LoginAuthMicroServices.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LoginAuthMicroServices.Business.Abstract
{
    public interface IUserService
    {
        IDataResult<AccessToken> Login(LoginModel loginModel);

        IResult Register(RegisterModel registerModel);
    }
}