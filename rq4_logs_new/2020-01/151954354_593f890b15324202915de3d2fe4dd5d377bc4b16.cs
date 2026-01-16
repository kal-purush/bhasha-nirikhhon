using System;
using System.Collections.Generic;
using System.Text;
// ReSharper disable InconsistentNaming

namespace LambdaTester
{
    class ApiSettings
    {
        public string lambda_parser_endpoint_url { get; set; }
        public string lambda_parser_endpoint_secret { get; set; }
        public string lambda_parser_endpoint_access { get; set; }
    }
}