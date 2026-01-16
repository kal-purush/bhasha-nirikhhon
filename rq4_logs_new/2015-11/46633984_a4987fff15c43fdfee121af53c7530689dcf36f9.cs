using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace UekHD
{
    
    class CeneoProductComment : IProductComment
    {
        public CeneoProductComment(string comment)
        {
            m_comment = comment;
        }
        public string toString()
        {
            return m_comment;
        }
        private string m_comment;
    }
}