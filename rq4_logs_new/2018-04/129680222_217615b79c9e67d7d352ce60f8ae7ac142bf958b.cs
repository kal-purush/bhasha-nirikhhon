using System;
namespace _Homework
{
    public class Course
    {
        //<20180406-SG->public int maxStudent = 1;
        public int maxStudent = 10; //<20180406-SG+ Temp setting to 10>
		public string className;
		public int credit = 3;

		public Course(string name)
		{
			this.className = name;
		}
    }
}