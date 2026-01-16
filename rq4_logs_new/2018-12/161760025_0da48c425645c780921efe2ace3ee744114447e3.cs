using SEP_framwork.Views.FormData;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SEP_framwork
{
    public class DeleteForm : BaseForm
    {
        public DeleteForm(string cnnString, string nameTable) : base(cnnString, nameTable)
        {
        }
    }
}