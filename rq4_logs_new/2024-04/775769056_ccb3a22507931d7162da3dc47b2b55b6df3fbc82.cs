using LagDaemon.YAMUD.Model.User;
using Microsoft.VisualBasic;

namespace LagDaemon.YAMUD.Model.User;

public abstract class DataMask<TDto, TDao>
    where TDto : class
    where TDao : class
{
    public abstract TDto Map(TDao dao);
}

public class UserAccountMask : DataMask<UserAccountDto, UserAccount>
{
    
    public UserAccountMask()
    {
    }

    public override UserAccountDto Map(UserAccount dao)
    {  
        return new UserAccountDto()
        {
            Id = dao.ID,
            DisplayName = dao.DisplayName,
            Email = dao.EmailAddress,
            Role = (dao.UserRoles.Select(r => r.Role).Min()).ToString()
        };
    }

}