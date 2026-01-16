using QRisto.Application.Utils;

public class UserErrors
{
    public static readonly Error NotAuthorized = new(
        "Users.NotAuthorized", "There is no authorized users.");
    
    public static readonly Error UnableRegister = new(
        "Users.UnableRegister", "Can't create a new user.");
    
    public static readonly Error UnableLogin = new(
        "Users.UnableLogin", "The problem occurs while trying to login.");
    
    public static readonly Error UnableUpdate = new(
        "Users.UnableUpdate", "Can't update the user.");
    
    public static readonly Error UnableGenerateToken = new(
        "Users.UnableGenerateToken", "Can't generate the new token for user.");
    
    public static readonly Error NotFound = new(
        "Users.NotFind", "Can't find the user.");
    
    public static readonly Error InvalidTokens = new(
        "Users.InvalidTokens", "Api and refresh tokens are invalid.");
}