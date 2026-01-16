using System.Collections.Generic;

namespace QTFK.Models.DBIO
{
    public abstract class AbstractDeleteQuery : IDBQueryDelete
    {
        protected string prefix;
        protected string table;
        protected IQueryFilter filter;

        public AbstractDeleteQuery()
        {
            this.prefix = "";
            this.table = "";
            this.filter = NullQueryFilter.Instance;
        }

        public string Prefix
        {
            get
            {
                return this.prefix;
            }
            set
            {
                Asserts.isSomething(value, $"Value for property {nameof(Prefix)} cannot be null.");
                this.prefix = value;
            }
        }

        public string Table
        {
            get
            {
                return this.table;
            }
            set
            {
                Asserts.isSomething(value, $"Value for property {nameof(Table)} cannot be null.");
                this.table = value.Trim();
            }
        }

        public IQueryFilter Filter
        {
            get
            {
                return this.filter;
            }
            set
            {
                this.filter = value ?? NullQueryFilter.Instance;
            }
        }

        public virtual string Compile()
        {
            string whereSegment;

            Asserts.isFilled(this.Table, $"Property '{nameof(this.Table)}' cannot be empty.");

            whereSegment = this.filter.Compile();
            whereSegment = string.IsNullOrWhiteSpace(whereSegment) ? "" : $"WHERE ({whereSegment})";

            return $@"
                DELETE FROM {this.prefix}[{this.table}]
                {whereSegment}
                ;";
        }

        public virtual IDictionary<string, object> getParameters()
        {
            return this.filter.getParameters();
        }
    }
}