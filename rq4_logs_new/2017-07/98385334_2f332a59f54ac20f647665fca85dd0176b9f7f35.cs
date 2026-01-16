using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace static_html_proxy.Controllers
{
    public class ProxyController : Controller
    {
        const string style_header = "text/css; charset=utf-8";
        const string js_header = "application/javascript";

        private readonly ProxyManagerProvider _proxyManager;



        public ProxyController()
        {
            this._proxyManager = new ProxyManager();
        }



        public IActionResult Css(string id)
        {
            return Content(this.GetContent(id), style_header);
        }

        public IActionResult Js(string id)
        {
            return Content(this.GetContent(id), js_header);
        }



        private string GetContent(string id)
        {
            return this._proxyManager.GetContent(id).Result;
        }
    }
}