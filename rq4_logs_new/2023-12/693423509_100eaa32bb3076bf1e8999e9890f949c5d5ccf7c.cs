using QLSV.DTO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QLSV.DAO
{
    internal class managercourseidDAO
    {
        private static managercourseidDAO instance;

        public static managercourseidDAO Instance
        {
            get
            {
                if (instance == null) instance = new managercourseidDAO();
                return managercourseidDAO.instance;
            }
            private set
            {
                managercourseidDAO.instance = value;
            }
        }

        private managercourseidDAO() { }

        public List<managercourseid> Loadprofilebycourseid(string id)
        {
            string query = "select * from getListProfilesByCourseId( :id )";
            DataTable data = DataProvider.Instance.ExcuteQuery(query, new object[] { id });
            List<managercourseid> courses = new List<managercourseid>();

            foreach (DataRow row in data.Rows)
            {
                managercourseid course = new managercourseid(row);
                courses.Add(course);
            }

            return courses;
        }
        public bool deletestudent(string mssv, string idcourse)
        {
            string query = "SELECT RejectCourse( :mssv , :idcourse )";
            var data = (bool)DataProvider.Instance.ExcuteScalar(query, new object[] { mssv, idcourse });
            return data;
        }
        public bool Addstudent(string mssv, string idcourse)
        {
            string query = "SELECT AcceptCourse( :mssv , :idcourse )";
            var data = (bool)DataProvider.Instance.ExcuteScalar(query, new object[] { mssv, idcourse });
            return data;
        }
    }
}