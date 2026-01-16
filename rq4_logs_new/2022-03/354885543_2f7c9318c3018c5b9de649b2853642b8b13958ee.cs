using CefSharp;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Horizon.Handlers {
    internal class RequestHandler : CefSharp.Handler.RequestHandler {

		private bool mailto(IWebBrowser chromiumWebBrowser, IBrowser browser, IFrame frame, IRequest request, bool userGesture, bool isRedirect) {
			Process.Start(request.Url);
			return true;
		}

		private bool callto(IWebBrowser chromiumWebBrowser, IBrowser browser, IFrame frame, IRequest request, bool userGesture, bool isRedirect) {
			Process.Start(request.Url);
			return true;
		}

		protected override bool OnBeforeBrowse(IWebBrowser chromiumWebBrowser, IBrowser browser, IFrame frame, IRequest request, bool userGesture, bool isRedirect) {
			if (request.Url.StartsWith("mailto:")) {
				return mailto(chromiumWebBrowser, browser, frame, request, userGesture, isRedirect);
			} else if (request.Url.StartsWith("tel:") || request.Url.StartsWith("callto:")) {
				return callto(chromiumWebBrowser, browser, frame, request, userGesture, isRedirect);
			}
			return base.OnBeforeBrowse(chromiumWebBrowser, browser, frame, request, userGesture, isRedirect);
		}
	}


}