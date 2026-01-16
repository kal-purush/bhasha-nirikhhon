using projeto_integrado_2_sem.Models;
using projeto_integrado_2_sem.Validators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace projeto_integrado_2_sem.Casters
{
    abstract class GenericTypeCaster<T> where T : IStorable
    {
        public MultipleAttributeValidationResult result = new MultipleAttributeValidationResult();
        public abstract T GetModel();
        public ValidationResult GetResult()
        {
            return result;
        }

        public bool Valid()
        {
            return result.Valid();
        }

        public void Reset()
        {
            result.Clear();
        }
    }
}