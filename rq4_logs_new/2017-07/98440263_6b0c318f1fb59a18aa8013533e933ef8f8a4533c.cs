using System.Web;
using MvcSample.DTO.Auth;

namespace MvcSample.Common.Helpers
{
    public class UserHelper
    {
        public static UserDto CurrentUser
        {
            get
            {
                if (HttpContext.Current.Session["AdminUser"] == null)
                {
                    return null;
                }

                return (UserDto)HttpContext.Current.Session["AdminUser"];
            }

            set
            {
                HttpContext.Current.Session["AdminUser"] = value;
            }

        }
    }
}