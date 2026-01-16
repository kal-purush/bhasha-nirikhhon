using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gestion_Taller {
    public class Teacher {
        public int Id;

        private String firstName;
        private String lastName;

        public String FirstName {
            get {
                return this.firstName;
            } set {
                this.firstName = value;
                DBConnection.Instance.changeTeacherFirstName(this.Id, value);
            }
        }

        public String LastName {
            get {
                return this.lastName;
            } set {
                this.lastName = value;
                DBConnection.Instance.changeTeacherLastName(this.Id, value);
            }
        }

        public String FullName {
            get {
                return firstName + " " + lastName;
            } set {

            }
        }

        public Teacher(int id, String firstname, String lastname) {
            this.Id = id;
            this.firstName = firstname;
            this.lastName = lastname;
            this.FullName = firstname + " " + lastname;
        }
    }
}