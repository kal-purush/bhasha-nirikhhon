namespace JustBlueberry.ApplicationExceptions
{
    using System;
    public class PowerElectronMissingException : ApplicationException
    {
        public PowerElectronMissingException(string message)
            : base(message)
        {
        }
    }
}