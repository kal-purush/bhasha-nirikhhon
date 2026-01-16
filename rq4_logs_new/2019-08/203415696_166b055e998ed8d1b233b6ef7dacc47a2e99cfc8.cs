using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NullableTypes
{
    class DatabaseReader
    {
        // Nullable data field.
        public int? numericValue = null;
        public bool? boolValue = true;

        // Note the nullable return type.
        public int? GetIntFromDatabase()
        {
            return numericValue;
        }

        // Note the nullable return type.
        public bool? GetBoolFromDatabase()
        {
            return boolValue;
        }
    }
}