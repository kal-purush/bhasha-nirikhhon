using System;
using Server.Infrastructure;

namespace Server.Exceptions
{
    /// <summary>
    /// Own exception class extends <see cref="System.Exception"/> to impelement business logic errors
    /// </summary>
    public class BusinessLogicException : Exception
    {
        private string _informationForUser { get; }
        public TypeBusinessException TypeException { get; }
        public string InformationForUser
        {
            get
            {
                return String.IsNullOrWhiteSpace(_informationForUser) ? base.Message : _informationForUser;
            }
        }

        public BusinessLogicException(TypeBusinessException type, string message, string informationForUser = null)
            : base(message)
        {
            TypeException = type;
            _informationForUser = informationForUser;
        }
    }
}