using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Xml.Serialization;//added 2/25
using System.IO;

namespace sharpAdvising
{
    
   
    /// <summary>
    ///CourseStack temporary garbage code I was playing with but contains a function I dont want to delete yet 
    /// </summary>
    public class CourseStack
    {

        public CourseStack(string department, string number, bool a)
        {
            completeStack = a;
            List<CourseStackNode> stack = new List<CourseStackNode>();
            CourseStackNode goal = new CourseStackNode();
            goal.depID = department;
            goal.numID = number;
            goal.Marker = null;
            stack.Add(goal);

            if (!completeStack)
            {//Read these values from the Degree Table
                string num;
                string dep;
                //Console.WriteLine("input department ID");
                //dep = Console.ReadLine();

                //Console.WriteLine("input number ID");
                //num = Console.ReadLine();


            
            }
        }

        //Change this make it multidimensional
        bool getPreReqs(string depID, string numID)
        {
            SqlCommand cmd = new SqlCommand();
            SqlDataReader reader;
            CourseStackNode next;

            cmd.CommandText = "dbo.read_by_dep_num";
            cmd.CommandType = System.Data.CommandType.StoredProcedure;
            cmd.Parameters.Add(new SqlParameter("@DepartmentID", depID));
            cmd.Parameters.Add(new SqlParameter("@NumberID", numID));
            cmd.Connection = SQLHANDLER.myConnection2;

            reader = cmd.ExecuteReader();
            int k = 0;
            while (reader.Read())
            {
                k++;
                int fieldCount = reader.FieldCount;
                for (int i = 0; i < fieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    string readValue = reader.GetValue(i).ToString();
                    if (colName == "PreReqDepartmentID")
                    {
                        next = new CourseStackNode();
                        next.depID = readValue;
                        stack.Add(next);
                    }
                    else if (colName == "PreReqNumberID")
                    {
                        stack[k].numID = readValue;
                    }
                    else if (colName == "Marker")
                    {
                        stack[k].Marker = readValue;
                    }
                    Console.WriteLine("we here");
                }

            }

            reader.Close();
            return true;
        }

        bool completeStack;



        public List<CourseStackNode> stack;
    }
}