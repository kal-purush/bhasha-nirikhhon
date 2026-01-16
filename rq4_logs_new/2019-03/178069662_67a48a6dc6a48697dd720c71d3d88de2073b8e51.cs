using System;
using System.Collections.Generic;
using System.Text;

namespace BugTracker.DataAccess.Requests
{
    public class CreateBugRequest
    {
        public string Title { get; set; }

        public string Description { get; set; }
    }
}