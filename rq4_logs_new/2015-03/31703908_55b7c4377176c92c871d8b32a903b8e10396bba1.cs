using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLObjectBackup
{
    public class TableConstraints
    {
        public string ConstraintName { get; set; }
        public string TableName { get; private set; }
        public string ColumnName { get; private set; }
        public string TableReferenceName { get; set; }
        public string TableReferenceConstraintName { get; set; }
        public string TableReferenceColumnName { get; set; }

        public TableConstraints(string constraintName, string tableName, string columnName, string tableReferenceName, string tableReferenceConstraintName, string tableReferenceColumnName)
        {
            if (string.IsNullOrWhiteSpace(constraintName))
                throw new ArgumentException("ObjectID cannot be zero");
            if (string.IsNullOrWhiteSpace(tableName))
                throw new ArgumentException("Schema name cannot be null or empty");
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentException("Object name cannot be null or empty");
            if (string.IsNullOrWhiteSpace(tableReferenceName))
                throw new ArgumentException("Fully quoted name cannot be null or empty");
            if (string.IsNullOrWhiteSpace(tableReferenceConstraintName))
                throw new ArgumentException("Fully quoted name cannot be null or empty");
            if (string.IsNullOrWhiteSpace(tableReferenceColumnName))
                throw new ArgumentException("Fully quoted name cannot be null or empty");

            ConstraintName = constraintName;
            TableName = tableName;
            ColumnName = columnName;
            TableReferenceName = tableReferenceName;
            TableReferenceConstraintName = tableReferenceConstraintName;
            TableReferenceColumnName = tableReferenceColumnName;
        }
    }
}