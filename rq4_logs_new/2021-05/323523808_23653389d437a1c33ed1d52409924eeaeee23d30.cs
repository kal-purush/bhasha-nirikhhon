using MyRestaurant.Business.Errors;
using MyRestaurant.Services;
using System;
using System.Net;

namespace MyRestaurant.Business.Repositories.Common
{
    public class Helper
    {
        public static CurrentUser GetCurrentUser(IUserAccessorService userAccessor)
        {
            var user = userAccessor.GetCurrentUser();

            if (user == null || user.UserId == Guid.Empty)
                throw new RestException(HttpStatusCode.BadRequest, "User details not found. Login again.");

            return user;
        }
    }
}