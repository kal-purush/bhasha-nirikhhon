using System;

namespace OSK.Math.Polynomials.Exceptions
{
    public class ModInverseException: InvalidOperationException
    {
        public ModInverseException(string message)
            : base(message)
        {
        }
    }
}