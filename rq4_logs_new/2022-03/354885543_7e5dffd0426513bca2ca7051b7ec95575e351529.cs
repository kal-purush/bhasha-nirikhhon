using CefSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Horizon.Handlers {
    internal class SchemeHandlerFactory : ISchemeHandlerFactory {
        private static readonly IDictionary<string, string> ResourceDictionary;

		static SchemeHandlerFactory() {
			Console.WriteLine(App.appPath + "/IncludeFiles/Pages/NewTab.html" + " jsjsjdjdjdjd");
			ResourceDictionary = new Dictionary<string, string>
			{
				{ "/newtab", App.appPath + "/IncludeFiles/Pages/NewTab.html" },
			};
		}

		public IResourceHandler Create(IBrowser browser, IFrame frame, string schemeName, IRequest request) {
			var uri = new Uri(request.Url);
			var fileName = uri.AbsolutePath;

			string resource;
			if (ResourceDictionary.TryGetValue(fileName, out resource) && !string.IsNullOrEmpty(resource)) {
				Console.WriteLine("IS a horizon:// link");
				var fileExtension = Path.GetExtension(fileName);
				return ResourceHandler.FromString(resource, mimeType: Cef.GetMimeType(fileExtension));
			}

			return null;
		}
    }
}