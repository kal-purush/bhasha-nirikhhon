namespace SoftmakeAll.SDK.WebAPI
{
  #region Authorize: PowerAccount
  [Microsoft.AspNetCore.Authorization.Authorize(Roles = "PowerAccount")]
  public abstract class PowerAccountControllerBase : SoftmakeAll.SDK.WebAPI.ControllerBase
  {
    #region Constructor
    public PowerAccountControllerBase(SoftmakeAll.SDK.DataAccess.ConnectorBase DatabaseInstanceContext) : base(DatabaseInstanceContext) { }
    #endregion
  }
  #endregion

  #region Authorize: PowerAccount or DeveloperAccount
  [Microsoft.AspNetCore.Authorization.Authorize(Roles = "PowerAccount,DeveloperAccount")]
  public abstract class DeveloperAccountControllerBase : SoftmakeAll.SDK.WebAPI.ControllerBase
  {
    #region Constructor
    public DeveloperAccountControllerBase(SoftmakeAll.SDK.DataAccess.ConnectorBase DatabaseInstanceContext) : base(DatabaseInstanceContext) { }
    #endregion
  }
  #endregion

  #region Authorize: PowerAccount or DeveloperAccount or AdminAccount
  [Microsoft.AspNetCore.Authorization.Authorize(Roles = "PowerAccount,DeveloperAccount,AdminAccount")]
  public abstract class AdminAccountControllerBase : SoftmakeAll.SDK.WebAPI.ControllerBase
  {
    #region Constructor
    public AdminAccountControllerBase(SoftmakeAll.SDK.DataAccess.ConnectorBase DatabaseInstanceContext) : base(DatabaseInstanceContext) { }
    #endregion
  }
  #endregion

  #region Authorize: PowerAccount or DeveloperAccount or AdminAccount or UserAccount
  [Microsoft.AspNetCore.Authorization.Authorize(Roles = "PowerAccount,DeveloperAccount,AdminAccount,UserAccount")]
  public abstract class UserAccountControllerBase : SoftmakeAll.SDK.WebAPI.ControllerBase
  {
    #region Constructor
    public UserAccountControllerBase(SoftmakeAll.SDK.DataAccess.ConnectorBase DatabaseInstanceContext) : base(DatabaseInstanceContext) { }
    #endregion
  }
  #endregion

  #region Authorize: Any authenticated user
  [Microsoft.AspNetCore.Authorization.Authorize()]
  public abstract class AuthorizeControllerBase : SoftmakeAll.SDK.WebAPI.ControllerBase
  {
    #region Constructor
    public AuthorizeControllerBase(SoftmakeAll.SDK.DataAccess.ConnectorBase DatabaseInstanceContext) : base(DatabaseInstanceContext) { }
    #endregion
  }
  #endregion
}