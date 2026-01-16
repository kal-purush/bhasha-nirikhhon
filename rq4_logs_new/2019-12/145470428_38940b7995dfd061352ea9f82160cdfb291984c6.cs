using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Razor.TagHelpers;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace THNETII.WebServices.AspNetCore.TagHelpers
{
    [HtmlTargetElement(Attributes = ActiveAttributePrefix + "*")]
    public class ActiveRouteTagHelper : TagHelper
    {
        private const string ActiveAttributePrefix = "asp-active-";

        private const string ActionAttributeName = ActiveAttributePrefix + "action";
        private const string ControllerAttributeName = ActiveAttributePrefix + "controller";
        private const string AreaAttributeName = ActiveAttributePrefix + "area";
        private const string PageAttributeName = ActiveAttributePrefix + "page";
        private const string PageHandlerAttributeName = ActiveAttributePrefix + "page-handler";
        private const string FragmentAttributeName = ActiveAttributePrefix + "fragment";
        private const string HostAttributeName = ActiveAttributePrefix + "host";
        private const string ProtocolAttributeName = ActiveAttributePrefix + "protocol";
        private const string RouteAttributeName = ActiveAttributePrefix + "route";
        private const string RouteValuesDictionaryName = ActiveAttributePrefix + "all-route-data";
        private const string RouteValuesPrefix = ActiveAttributePrefix + "route-";

        private const string CssClassAttributeName = ActiveAttributePrefix + "class";
        private const string OnlyActiveOutputAttributeName = ActiveAttributePrefix + "only";

        private IDictionary<string, string> _routeValues;

        public ActiveRouteTagHelper() : base()
        {
        }

        /// <summary>
        /// The CSS class to add if the currently executing route matches the route specified by the attributes of the tag.
        /// </summary>
        [HtmlAttributeName(CssClassAttributeName)]
        public string AddCssClass { get; set; }

        /// <summary>
        /// Only render the tag if the currently executing route matches the route specified by the attributes of the tag.
        /// </summary>
        [HtmlAttributeName(OnlyActiveOutputAttributeName)]
        public bool SuppressInactiveOutput { get; set; }

        /// <summary>
        /// The name of the action method.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if <see cref="Route"/> or <see cref="Page"/> is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(ActionAttributeName)]
        public string Action { get; set; }

        /// <summary>
        /// The name of the controller.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if <see cref="Route"/> or <see cref="Page"/> is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(ControllerAttributeName)]
        public string Controller { get; set; }

        /// <summary>
        /// The name of the area.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if <see cref="Route"/> is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(AreaAttributeName)]
        public string Area { get; set; }

        /// <summary>
        /// The name of the page.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if <see cref="Route"/> or <see cref="Action"/>, <see cref="Controller"/>
        /// is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(PageAttributeName)]
        public string Page { get; set; }

        /// <summary>
        /// The name of the page handler.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if <see cref="Route"/> or <see cref="Action"/>, or <see cref="Controller"/>
        /// is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(PageHandlerAttributeName)]
        public string PageHandler { get; set; }

        /// <summary>
        /// The protocol for the URL, such as &quot;http&quot; or &quot;https&quot;.
        /// </summary>
        [HtmlAttributeName(ProtocolAttributeName)]
        public string Protocol { get; set; }

        /// <summary>
        /// The host name.
        /// </summary>
        [HtmlAttributeName(HostAttributeName)]
        public string Host { get; set; }

        /// <summary>
        /// The URL fragment name.
        /// </summary>
        [HtmlAttributeName(FragmentAttributeName)]
        public string Fragment { get; set; }

        /// <summary>
        /// Name of the route.
        /// </summary>
        /// <remarks>
        /// Must be <c>null</c> if one of <see cref="Action"/>, <see cref="Controller"/>, <see cref="Area"/> 
        /// or <see cref="Page"/> is non-<c>null</c>.
        /// </remarks>
        [HtmlAttributeName(RouteAttributeName)]
        public string Route { get; set; }

        /// <summary>
        /// Additional parameters for the route.
        /// </summary>
        [HtmlAttributeName(RouteValuesDictionaryName, DictionaryAttributePrefix = RouteValuesPrefix)]
        [SuppressMessage("Usage", "CA2227: Collection properties should be read only")]
        public IDictionary<string, string> RouteValues
        {
            get
            {
                if (_routeValues == null)
                {
                    _routeValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                }

                return _routeValues;
            }
            set
            {
                _routeValues = value;
            }
        }

        /// <summary>
        /// Gets or sets the <see cref="Rendering.ViewContext"/> for the current request.
        /// </summary>
        [HtmlAttributeNotBound]
        [ViewContext]
        public ViewContext ViewContext { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (output == null)
                throw new ArgumentNullException(nameof(output));

            bool isActive = IsActive();
            if (SuppressInactiveOutput && !isActive)
            {
                output.SuppressOutput();
                return;
            }

            if (!string.IsNullOrEmpty(AddCssClass) && isActive)
            {
                string newClassValue;
                if (output.Attributes.TryGetAttribute("class", out var classAttribute))
                {
                    var oldClassValue = classAttribute.Value?.ToString();
                    newClassValue = string.IsNullOrEmpty(oldClassValue)
                        ? AddCssClass
                        : oldClassValue + ' ' + AddCssClass;
                }
                else
                    newClassValue = AddCssClass;
                output.Attributes.SetAttribute("class", newClassValue);
            }
        }

        private bool IsActive()
        {
            var requestRouteValues = ViewContext.RouteData.Values;

            if (!MatchRequestRouteValue(Area, requestRouteValues, "area"))
                return false;

            if (!MatchRequestRouteValue(Page, requestRouteValues, "page"))
                return false;

            if (!MatchRequestRouteValue(PageHandler, requestRouteValues, "page-handler"))
                return false;

            if (!MatchRequestRouteValue(Controller, requestRouteValues, "controller"))
                return false;

            if (!MatchRequestRouteValue(Action, requestRouteValues, "action"))
                return false;

            foreach (var matchRouteValue in _routeValues ?? Enumerable.Empty<KeyValuePair<string, string>>())
            {
                if (!MatchRequestRouteValue(matchRouteValue.Value, requestRouteValues, matchRouteValue.Key))
                    return false;
            }

            return true;

            static bool MatchRequestRouteValue(string matchValue, RouteValueDictionary requestRouteValues, string requestValueKey)
            {
                if (matchValue is null)
                    return true;
                _ = requestRouteValues.TryGetValue(requestValueKey, out var requestValueData);
                if (!string.Equals(matchValue, requestValueData?.ToString() ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                    return false;
                return true;
            }
        }
    }
}