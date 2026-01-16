using System;
using DotNetCafe.Internals;

namespace DotNetCafe
{
    public readonly struct Cpf : IComparable, IComparable<Cpf>, IEquatable<Cpf>, IFormattable
    {
        public static readonly Cpf Empty;

        public Cpf(string s)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(object obj) =>
            throw new NotImplementedException();
        
        public int CompareTo(Cpf other) =>
            throw new NotImplementedException();
        
        public bool Equals(Cpf value) =>
            throw new NotImplementedException();
        
        public override bool Equals(object obj) =>
            throw new NotImplementedException();
        
        public override int GetHashCode() =>
            throw new NotImplementedException();
        
        public override string ToString() =>
            throw new NotImplementedException();

        public string ToString(string format) =>
            throw new NotImplementedException();
        
        public string ToString(IFormatProvider formatProvider) =>
            throw new NotImplementedException();
        
        public string ToString(string format, IFormatProvider formatProvider) =>
            throw new NotImplementedException();
        
        public static Cnpj Parse(string s) =>
            throw new NotImplementedException();
        
        public static bool TryParse(string s, out Cnpj result) =>
            throw new NotImplementedException();
        
        public static bool IsEmpty(Cpf other) =>
            throw new NotImplementedException();
        
        public static bool operator ==(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }

        public static bool operator !=(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }

        public static bool operator >(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }

        public static bool operator <(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }

        public static bool operator >=(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }

        public static bool operator <=(Cpf lhs, Cpf rhs)
        {
            throw new NotImplementedException();
        }
    }
}