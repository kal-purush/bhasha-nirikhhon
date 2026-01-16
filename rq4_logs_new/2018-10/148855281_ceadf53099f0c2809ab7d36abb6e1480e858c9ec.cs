using SymbioticTS.Core.Visitors;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SymbioticTS.Core
{
    internal class DtoInterfaceTransformLookup : IReadOnlyDictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>
    {
        private readonly Dictionary<TsTypeSymbol, DtoInterfaceTransformMetadata> lookup = new Dictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>();

        /// <summary>
        /// Gets the number of elements in the collection.
        /// </summary>
        public int Count => this.lookup.Count;

        /// <summary>
        /// Gets an enumerable collection that contains the keys in the read-only dictionary.
        /// </summary>
        public IEnumerable<TsTypeSymbol> Keys => ((IReadOnlyDictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>)this.lookup).Keys;

        /// <summary>
        /// Gets an enumerable collection that contains the values in the read-only dictionary.
        /// </summary>
        public IEnumerable<DtoInterfaceTransformMetadata> Values => ((IReadOnlyDictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>)this.lookup).Values;

        /// <summary>
        /// Gets the <see cref="DtoInterfaceTransformMetadata"/> with the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns><see cref="DtoInterfaceTransformMetadata"/>.</returns>
        public DtoInterfaceTransformMetadata this[TsTypeSymbol key] => this.lookup[key];

        /// <summary>
        /// Prevents a default instance of the <see cref="DtoInterfaceTransformLookup"/> class from being created.
        /// </summary>
        private DtoInterfaceTransformLookup()
        {
        }

        /// <summary>
        /// Builds a <see cref="DtoInterfaceTransformLookup"/> from the class symbol.
        /// </summary>
        /// <param name="classSymbol">The class symbol.</param>
        /// <returns>TsDtoInterfaceTransformLookup.</returns>
        public static DtoInterfaceTransformLookup BuildLookup(TsTypeSymbol classSymbol)
        {
            if (classSymbol == null)
            {
                throw new ArgumentNullException(nameof(classSymbol));
            }

            if (!classSymbol.IsClass)
            {
                throw new ArgumentException("The symbol must be a class.", nameof(classSymbol));
            }

            if (classSymbol.IsAbstractClass)
            {
                throw new ArgumentException("The symbol must not be an abstract class.", nameof(classSymbol));
            }

            TsDirectDependencyTypeSymbolVisitor dependencyVisitor = new TsDirectDependencyTypeSymbolVisitor();
            DtoInterfaceTransformLookup lookup = new DtoInterfaceTransformLookup();

            foreach (TsPropertySymbol propertySymbol in classSymbol.Properties)
            {
                TsTypeSymbol unwrappedTypeSymbol = propertySymbol.Type.UnwrapArray();

                if (DtoInterfaceTransformLookup.NeedsInterfaceTransform(unwrappedTypeSymbol))
                {
                    lookup.Add(classSymbol, unwrappedTypeSymbol);
                }

                foreach (TsTypeSymbol ptdSymbol in dependencyVisitor.GetDependencies(unwrappedTypeSymbol)
                    .Where(DtoInterfaceTransformLookup.NeedsInterfaceTransform))
                {
                    lookup.Add(classSymbol, ptdSymbol);
                }
            }

            return lookup;
        }

        /// <summary>
        /// Determines whether the read-only dictionary contains an element that has the specified key.
        /// </summary>
        /// <param name="key">The key to locate.</param>
        /// <returns>true if the read-only dictionary contains an element that has the specified key; otherwise, false.</returns>
        public bool ContainsKey(TsTypeSymbol key)
        {
            return this.lookup.ContainsKey(key);
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>An enumerator that can be used to iterate through the collection.</returns>
        public IEnumerator<KeyValuePair<TsTypeSymbol, DtoInterfaceTransformMetadata>> GetEnumerator()
        {
            return ((IReadOnlyDictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>)this.lookup).GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An <see cref="T:System.Collections.IEnumerator"></see> object that can be used to iterate through the collection.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IReadOnlyDictionary<TsTypeSymbol, DtoInterfaceTransformMetadata>)this.lookup).GetEnumerator();
        }

        /// <summary>
        /// Gets the value that is associated with the specified key.
        /// </summary>
        /// <param name="key">The key to locate.</param>
        /// <param name="value">When this method returns, the value associated with the specified key, if the key is found; otherwise, the default value for the type of the value parameter. This parameter is passed uninitialized.</param>
        /// <returns>true if the object that implements the <see cref="T:System.Collections.Generic.IReadOnlyDictionary`2"></see> interface contains an element that has the specified key; otherwise, false.</returns>
        public bool TryGetValue(TsTypeSymbol key, out DtoInterfaceTransformMetadata value)
        {
            return this.lookup.TryGetValue(key, out value);
        }

        /// <summary>
        /// Gets a value indicating if the specified <see cref="TsTypeSymbol"/> needs a DTO interface transform.
        /// </summary>
        /// <param name="typeSymbol">The type symbol.</param>
        /// <returns><c>true</c> if the <see cref="TsTypeSymbol"/> needs a DTO interface transform, <c>false</c> otherwise.</returns>
        private static bool NeedsInterfaceTransform(TsTypeSymbol typeSymbol)
        {
            return typeSymbol.HasDtoInterface
                   && typeSymbol.IsInterface
                   && typeSymbol.RequiresDtoTransform();
        }

        private void Add(TsTypeSymbol classSymbol, TsTypeSymbol typeSymbol)
        {
            if (!NeedsInterfaceTransform(typeSymbol))
            {
                throw new NotSupportedException("Only symbols that needd interface transform can be added.");
            }

            if (this.lookup.ContainsKey(typeSymbol))
            {
                return;
            }

            this.lookup[typeSymbol.DtoInterface] = new DtoInterfaceTransformMetadata
            {
                ClassSymbol = typeSymbol,
                TransformMethodAccessor = $"{classSymbol.Name}.from{typeSymbol.DtoInterface.Name}",
                TransformMethodName = $"from{typeSymbol.DtoInterface.Name}"
            };
        }
    }
}