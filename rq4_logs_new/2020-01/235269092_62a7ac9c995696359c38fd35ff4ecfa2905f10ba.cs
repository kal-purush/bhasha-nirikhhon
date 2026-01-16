using System;
using DotNetCafe.Internals;

namespace DotNetCafe
{
    public readonly struct Cnpj : IComparable, IComparable<Cnpj>, IEquatable<Cnpj>, IFormattable
    {
        public static readonly Cnpj Empty = new Cnpj();

        internal readonly long value;

        internal Cnpj(long number)
        {
            value = number;
        }

        public Cnpj(string s)
        {
            this = CnpjParser.Parse(s);
        }

        public int CompareTo(object obj) => 
            CnpjComparer.Compare(this, obj);

        public int CompareTo(Cnpj other) => 
            CnpjComparer.Compare(this, other);
        
        public bool Equals(Cnpj value) => 
            CnpjEqualityComparer.Equals(this, value);

        public override bool Equals(object obj) => 
            CnpjEqualityComparer.Equals(this, obj);

        public override int GetHashCode() => 
            CnpjEqualityComparer.GetHashCode(this);

        public override string ToString() => 
            CnpjFormatter.Format(this, null!, null!);
        
        public string ToString(string format) => 
            CnpjFormatter.Format(this, format, null!);
        
        public string ToString(IFormatProvider formatProvider) => 
            CnpjFormatter.Format(this, null!, formatProvider);
        
        public string ToString(string format, IFormatProvider formatProvider) => 
            CnpjFormatter.Format(this, format,  formatProvider);
        
        public static Cnpj Parse(string s) => 
            CnpjParser.Parse(s);
        
        public static bool TryParse(string s, out Cnpj result) => 
            CnpjParser.TryParse(s, out result);
        
        public static bool IsEmpty(Cnpj other) => 
            CnpjEqualityComparer.Equals(Empty, other);

        public static bool operator ==(Cnpj lhs, Cnpj rhs)
        {
            return CnpjEqualityComparer.Equals(lhs, rhs);
        }

        public static bool operator !=(Cnpj lhs, Cnpj rhs)
        {
            return !CnpjEqualityComparer.Equals(lhs, rhs);
        }

        public static bool operator >(Cnpj lhs, Cnpj rhs)
        {
            return CnpjComparer.Compare(lhs, rhs) > 0;
        }

        public static bool operator <(Cnpj lhs, Cnpj rhs)
        {
            return CnpjComparer.Compare(lhs, rhs) < 0;
        }

        public static bool operator >=(Cnpj lhs, Cnpj rhs)
        {
            return CnpjComparer.Compare(lhs, rhs) >= 0;
        }

        public static bool operator <=(Cnpj lhs, Cnpj rhs)
        {
            return CnpjComparer.Compare(lhs, rhs) <= 0;
        }
    }
}