using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Xml.Serialization;
using System.IO;

namespace sharpAdvising
{
    public class Subject
    {
        public Subject()
        {
            coursesReq = new List<Course>();
        }
        public Subject(string dept)
        {
            coursesReq = new List<Course>();
            department = dept;
            addCourseRequirementsForDepartment();
        }

     public void addCourseRequirementsForDepartment()
        {
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;

            cmd.CommandText = "dbo.read_courseID_by_program_department";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@DepartmentID", department));
            cmd.Parameters.Add(new SqlParameter("@program", PreReq.program));
            cmd.Connection = SQLHANDLER.myConnection2;
            
            reader = cmd.ExecuteReader();

            string readValue;
            while(reader.Read())
            {
                readValue = reader.GetValue(0).ToString();
                coursesReq.Add((new Course(readValue, department)));
            }
            reader.Close();    
        }

        
        public List<Course> coursesReq;
        public string department;
    }
}