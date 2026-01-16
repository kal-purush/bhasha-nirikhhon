using System;
using System.Collections.Generic;

namespace HeroisDoCancer.Services.Exceptions
{
    public class CamposInvalidosException : Exception
    {
        private readonly IDictionary<String, String[]> campos;

        public CamposInvalidosException(IDictionary<String, String[]> campos)
        {
            if (campos == null)
                throw new ArgumentNullException("campos");

            this.campos = campos;
        }

        public IDictionary<String, String[]> Campos
        {
            get { return campos; }
        }
    }
}