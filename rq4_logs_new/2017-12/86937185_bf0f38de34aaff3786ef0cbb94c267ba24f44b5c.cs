using System;
using QTFK.Models;
using System.Collections.Generic;
using System.Linq;

namespace QTFK.Services.DBIO
{
    public abstract class AbstractQueryFactory : IQueryFactory
    {
        protected readonly List<Type> filterTypes;
        protected readonly string queryFilterInterfaceTypeFullName;

        public AbstractQueryFactory(IEnumerable<Type> filterTypes)
        {
            this.queryFilterInterfaceTypeFullName = typeof(IQueryFilter).FullName;
            this.filterTypes = filterTypes.ToList();

            foreach (Type t in this.filterTypes)
            {
                Asserts.isSomething(t, $"Type element at parameter 'filterTypes' is null!");
                Asserts.check(t.IsInterface == false, $"Type '{t.FullName}' at parameter 'filterTypes' cannot be interface.");
                Asserts.check(t.GetInterface(this.queryFilterInterfaceTypeFullName) != null, $"Type '{t.FullName}' at parameter 'filterTypes' does not implements '{this.queryFilterInterfaceTypeFullName}'.");
            }
        }

        public string Prefix { get; set; }

        public abstract IDBQueryDelete newDelete();
        public abstract IDBQueryInsert newInsert();
        public abstract IDBQuerySelect newSelect();
        public abstract IDBQueryUpdate newUpdate();

        public IQueryFilter buildFilter(Type type)
        {
            Type filterType;
            object instance;

            Asserts.check(type.IsInterface, $"Type '{type.FullName}' is not an interface.");
            Asserts.check(type.GetInterface(this.queryFilterInterfaceTypeFullName) != null, $"Type '{type.FullName}' does not inherits from '{this.queryFilterInterfaceTypeFullName}'.");

            filterType = this.filterTypes
                .Single(t => t.IsAssignableFrom(type));

            instance = Activator.CreateInstance(filterType);
            return (IQueryFilter)instance;
        }
    }
}