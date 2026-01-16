using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OrderSystem
{
    class Customer
    {
        int _ID;
        string _firstName;
        string _lastName;
        string _telephone;
        string _address;

        public int ID
        {
            get
            {
                return this._ID;
            }

            set
            {
                ID = value;
            }

        }
        public string FirstName
        {
            get
            {
                return _firstName;
            }
            set
            {
                FirstName = value;
            }
        }
        public string LastName
        {
            get
            {
                return _lastName;
            }
            set
            {
                LastName = value;
            }
        }
        public string Telephone
        {
            get
            {
                return _telephone;
            }
            set
            {
                Telephone = value;
            }
        }
        public string Address
        {
            get
            {
                return _address;
            }
            set
            {
                Address = value;
            }
        }
        /// <summary>
        /// Epistrefei panta times
        /// </summary>
        public Customer()    //Base Constructor (private fields not null at all)
        {
            _ID = 0;
            _firstName = "_";
            _lastName = "_";
            _telephone = "_";
            _address = "_";
        }


        public override string ToString()
        {
            return ID + " " + FirstName + " " + LastName + " " + Telephone + " " + Address;
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }
            if (!(obj is Customer))
            {
                return false;
            }

            return this.FirstName == ((Customer)obj).FirstName &&
            this.LastName == ((Customer)obj).LastName;
        }

        public override int GetHashCode()
        {
            return this._ID.GetHashCode() ^ this._firstName.GetHashCode() ^ this._lastName.GetHashCode() ^
                this._telephone.GetHashCode() ^ this._address.GetHashCode();
        }
    }
}