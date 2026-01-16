using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Razor.TagHelpers;

namespace WebStore.TagHelpers
{
    [HtmlTargetElement(Attributes = AttributeName)]
    public class ActiveRouteTagHelper : TagHelper
    {
        private const string AttributeName = "is-active-route";
        private const string IgnoreActionName = "ignore-action";

        private IDictionary<string, string> _routeValues;

        [HtmlAttributeName("asp-action")]
        public string Action { get; set; }

        [HtmlAttributeName("asp-controller")]
        public string Controller { get; set; }

        [HtmlAttributeName("asp-all-route-data", DictionaryAttributePrefix = "asp-route-")]
        public IDictionary<string, string> RouteValues
        {
            get { return _routeValues ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase); }
            set => _routeValues = value;
        }

        [HtmlAttributeNotBound] [ViewContext] public ViewContext ViewContext { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            base.Process(context, output);

            var shouldIgnoreAction = context.AllAttributes.ContainsName(IgnoreActionName);

            if (ShouldBeActive(shouldIgnoreAction)) MakeActive(output);

            output.Attributes.RemoveAll(AttributeName);
            output.Attributes.RemoveAll(IgnoreActionName);
        }

        private bool ShouldBeActive(bool shouldIgnoreAction)
        {
            var routeValues = ViewContext.RouteData.Values;
            var currentController = routeValues["Controller"].ToString();
            var currentAction = routeValues["Action"].ToString();

            if (!string.IsNullOrWhiteSpace(Controller) &&
                !string.Equals(Controller, currentController, StringComparison.OrdinalIgnoreCase)) return false;

            if (!shouldIgnoreAction && !string.IsNullOrWhiteSpace(Action) &&
                !string.Equals(Action, currentAction, StringComparison.OrdinalIgnoreCase)) return false;

            foreach (var (key, value) in RouteValues)
                if (!routeValues.ContainsKey(key) || routeValues[key].ToString() != value)
                    return false;

            return true;
        }

        private static void MakeActive(TagHelperOutput output)
        {
            var classAttr = output.Attributes.FirstOrDefault(a => a.Name == "class");
            if (classAttr == null)
            {
                classAttr = new TagHelperAttribute("class", "active");
                output.Attributes.Add(classAttr);
            }
            else if (classAttr.Value.ToString()?.Contains("active") ?? false)
            {
                output.Attributes.SetAttribute("class", classAttr.Value == null
                    ? "active"
                    : classAttr.Value + " active");
            }
        }
    }
}