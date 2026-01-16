using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace BusinessModel.GlobalModels
{
    public class ApiResponse
    {
        private bool _success;
        public bool Success
        {

            get => (Message == null || (Message.Type != MessageType.Error && Message.Type != MessageType.Fatal)) && _success;
            set => _success = value;
        }
        public ApiMessage? Message { get; set; }
        public List<ApiMessage>? Messages { get; set; }

        public int StatusCode { get; set; }

        public ApiResponse()
        {
            _success = true;
            StatusCode = (int)HttpStatusCode.OK;
        }
    } 
}

    

