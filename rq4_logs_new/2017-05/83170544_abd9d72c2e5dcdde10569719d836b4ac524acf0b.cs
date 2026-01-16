using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Slalom.Stacks.Services;
using Slalom.Stacks.Services.Inventory;

namespace Slalom.Stacks.AspNetCore.EndPoints
{

    public class RemoteEndPoint
    {
        public string Path { get; set; }

        public string FullPath { get; set; }

        public RemoteEndPoint(string path, string fullPath)
        {
            this.Path = path;
            this.FullPath = fullPath;
        }
    }

    [EndPoint("_system/endpoints")]
    public class GetEndPoints : EndPoint
    {
        private readonly ServiceInventory _services;
        private readonly IHttpContextAccessor _context;

        public GetEndPoints(ServiceInventory services, IHttpContextAccessor context)
        {
            _services = services;
            _context = context;
        }

        public override void Receive()
        {
            var url = this.GetBaseUrl();
            this.Respond(_services.EndPoints.Where(e => e.Public && !String.IsNullOrWhiteSpace(e.Path) && !e.Path.StartsWith("_"))
                .Select(e => new RemoteEndPoint(e.Path, url + "/" + e.Path)));
        }

        public string GetBaseUrl()
        {
            var request = _context.HttpContext.Request;

            var host = request.Host.ToUriComponent();

            var pathBase = request.PathBase.ToUriComponent();

            return $"{request.Scheme}://{host}{pathBase}";
        }
    }
}