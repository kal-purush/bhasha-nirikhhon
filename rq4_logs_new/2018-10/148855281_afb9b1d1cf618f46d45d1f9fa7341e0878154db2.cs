using System.Collections.Generic;
using System.Reflection;

namespace SymbioticTS.Core
{
    internal sealed class AssemblyNameComparer : IEqualityComparer<AssemblyName>
    {
        /// <summary>
        /// Gets the instance.
        /// </summary>
        public static AssemblyNameComparer Instance { get; } = new AssemblyNameComparer();

        /// <summary>
        /// Prevents a default instance of the <see cref="AssemblyNameComparer"/> class from being created.
        /// </summary>
        private AssemblyNameComparer() { }

        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <param name="x">The first object of type T to compare.</param>
        /// <param name="y">The second object of type T to compare.</param>
        /// <returns>true if the specified objects are equal; otherwise, false.</returns>
        public bool Equals(AssemblyName x, AssemblyName y)
        {
            if (object.ReferenceEquals(x, y))
            {
                return true;
            }

            if (x == null || y == null)
            {
                return false;
            }

            return x.FullName == y.FullName;
        }

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <param name="obj">The <see cref="T:System.Object"></see> for which a hash code is to be returned.</param>
        /// <returns>A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.</returns>
        public int GetHashCode(AssemblyName obj)
        {
            return obj.FullName.GetHashCode();
        }
    }
}