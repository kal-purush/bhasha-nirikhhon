using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Project.ValidationFarmework.violation
{
    public class CViolation : IViolation
    {
        private string prop;
        private object value;
        private string messeage;
        private bool valid = true;

        public CViolation(string prop, object value)
        {
            this.prop = prop;
            this.value = value;
        }

        public string getMessage()
        {
            return messeage;
        }

        public string getProp()
        {
            return prop;
        }

        public bool getValid()
        {
            return valid;
        }

        public void setMessage(string message)
        {
            this.messeage = message;
        }

        public void setValid(bool valid)
        {
            this.valid = valid;
        }

    }
}