using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Slalom.Stacks.Services;

namespace Slalom.Stacks.AspNetCore.EndPoints
{
    [EndPoint("_system/ip-address")]
    public class GetIPAddress : EndPoint
    {
        private readonly IHttpContextAccessor _accessor;

        public GetIPAddress(IHttpContextAccessor accessor)
        {
            _accessor = accessor;
        }

        public override void Receive()
        {
            var content = _accessor.HttpContext.Connection.RemoteIpAddress.MapToIPv4().ToString();

            this.Respond(content);
        }
    }
}