namespace Stable.Business.Concrete.Exceptions
{
    public class BusinessException : CustomBaseException
    {
        public BusinessException(string message, string errorCode) : base(message, errorCode)
        {

        }
    }
}