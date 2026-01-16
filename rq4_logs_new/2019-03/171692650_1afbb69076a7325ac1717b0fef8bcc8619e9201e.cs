using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AffirmOrderFormsService.ViewModels.Requests
{
    public class FormsListRequest
    {
        public Guid CorrelationGuid { get; set; }
        public string CallerOrgCode { get; set; }
        public string UserName { get; set; }
        public string OrderId { get; set; }

        public bool RequiredOnly { get; set; }

        public bool OptionalOnly { get; set; }
        public bool IncludePdfString { get; set; }
    }
}