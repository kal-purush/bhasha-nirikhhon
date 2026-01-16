namespace SeaBattleFramework
{ 
    public enum ClientErrorCode : short
    {
        OperationDenied = -3,
        OperationInvalid,
        InternalServerError,
        Ok,
        UsernameInUse,
        IncorretcUsernameOrPassword,
        UserCurrentlyLoggedIn,
        InvalidPeerId
    }
}