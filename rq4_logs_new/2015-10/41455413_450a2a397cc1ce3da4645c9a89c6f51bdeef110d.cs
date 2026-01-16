using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using projeto_integrado_2_sem.Validators;

namespace projeto_integrado_2_sem
{
    class GenericTypeCaster<T>
    {
        public ValidationResult CastAttributes(T record, Dictionary<string, string> attrs)
        {
            foreach(var pair in attrs)
            {
                var property = typeof(T).GetProperty(pair.Key);
                
                if (property.GetType() == typeof(string))
                {
                    property.SetValue(record, pair.Value.Trim());
                }
            }

            return null;
        }
    }
}