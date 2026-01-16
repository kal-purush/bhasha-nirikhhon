using System.Web;
using MvcExample.DTO.User;

namespace MvcExample.Common.Helpers
{
    public class UserHelper
    {
        public static UserDto CurrentUser
        {
            get
            {
                if (HttpContext.Current.Session["CurrentUser"] == null)
                {
                    return null;
                }
                return (UserDto)HttpContext.Current.Session["CurrentUser"];
            }
            set
            {
                HttpContext.Current.Session["CurrentUser"] = value;
            }
        }
    }
}