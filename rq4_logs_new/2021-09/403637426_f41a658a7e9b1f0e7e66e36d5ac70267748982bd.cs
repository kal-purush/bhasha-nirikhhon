using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassRoom
{
    class ClassRoom
    {
        #region Instance Fields
        private string _className;
        private DateTime _semesterStart;
        private List<Student> _classList;
        #endregion

        #region Constructor
        public ClassRoom()
        {
            
        }
        #endregion

        #region Properties
        public string ClassName
        {
            get { return _className; }
            set { _className = value; }
        }

        public DateTime SemesterStart
        {
            get { return _semesterStart; }
            set { _semesterStart = value; }
        }

        public List<Student> ClassList
        {
            get { return _classList; }
            set { _classList = value; }
        }
        #endregion


    }
}