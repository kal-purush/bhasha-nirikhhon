using System;

namespace MazeWalker.Domain
{
    public class IntIdentifer : IIdentifer
    {
        int Identifer;

        public IntIdentifer(int identifer)
        {
            Identifer = identifer;
        }

        public virtual bool Equals(IIdentifer other)
        {
            return other.GetIdentifierType().Equals(TypeCode.Int32) &&
                other.GetIdentifier().Equals(Identifer);
        }

        public override int GetHashCode()
        {
            return Identifer;
        }

        public virtual object GetIdentifier()
        {
            return Identifer;
        }

        public virtual TypeCode GetIdentifierType()
        {
            return TypeCode.Int32;
        }
    }
}