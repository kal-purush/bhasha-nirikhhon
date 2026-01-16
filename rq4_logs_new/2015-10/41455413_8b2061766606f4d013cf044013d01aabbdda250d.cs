using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using projeto_integrado_2_sem.Models;
using projeto_integrado_2_sem.Validators;

namespace projeto_integrado_2_sem.Casters
{
    class UserCaster : GenericTypeCaster<User>
    {
        private User user;

        public UserCaster(User user)
        {
            this.user = user;
        }

        public UserCaster() : this(new User()) { }

        public override User GetModel()
        {
            return this.user;
        }

        public void setName(string name)
        {
            this.user.name = name.Trim();
        }

        public void setPassword(string password)
        {
            this.user.password = password;
        }

        public void setCode(string code)
        {
            this.user.code = code.Trim();
        }

        public void setEmail(string email)
        {
            this.user.email = email.Trim();
        }

        public void setBirthDate(string date)
        {
            var parsed = new DateTime();

            if (DateTime.TryParse(date, out parsed))
            {
                this.user.birthDate = parsed;
            }
            else
            {
                this.result.AddError("birthDate", MultipleAttributeValidationResult.Error.INVALID_DATE);
            }
        }
    }
}