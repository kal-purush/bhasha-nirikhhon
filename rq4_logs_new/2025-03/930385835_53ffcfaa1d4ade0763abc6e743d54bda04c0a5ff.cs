using Microsoft.AspNetCore.Authorization;

namespace PetFamily.Accounts.Infrastructure;

public class PermissionAttribute : AuthorizeAttribute,  IAuthorizationRequirement
{
    public string Code { get; set; }
    
    public PermissionAttribute(string code) : base(code)
    {
        Code = code;
    }
}

/*
public class PermissionAttribute : AuthorizeAttribute, IAuthorizationRequirement
{
    public string Code { get; set; }
    
    public PermissionAttribute(string policyName) : base(policy: policyName)
    {
        Code = "Volunteer";
    }
}
*/

