using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.AspNetCore.Mvc.TagHelpers;
using Microsoft.AspNetCore.Mvc.ViewFeatures;
using Microsoft.AspNetCore.Razor.TagHelpers;
using Microsoft.AspNetCore.Routing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Encodings.Web;
using System.Threading.Tasks;

namespace WebStore.TagHelpers
{
    [HtmlTargetElement(Attributes = AttributeName)]
    public class ActiveRouteTagHelper : TagHelper
    {
        private const string AttributeName = "ws-is-active-route";
        private const string IgnoreAction = "ws-ignore-action";
        private IDictionary<string, string> _routeValues;

        [HtmlAttributeName("asp-action")]
        public string Action { get; set; }

        [HtmlAttributeName("asp-controller")]
        public string Controller { get; set; }

        //[HtmlAttributeName("asp-all-route-data"/*, DictionaryAttributePrefix = "asp-route-"*/)]
        //public Dictionary<string, string> RouteValues { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>Additional parameters for the route.</summary>
        [HtmlAttributeName("asp-all-route-data", DictionaryAttributePrefix = "asp-route-")]
        public IDictionary<string, string> RouteValues
        {
            get
            {
                if (this._routeValues == null)
                    this._routeValues = (IDictionary<string, string>)new Dictionary<string, string>((IEqualityComparer<string>)StringComparer.OrdinalIgnoreCase);
                return this._routeValues;
            }
            set
            {
                this._routeValues = value;
            }
        }

        [ViewContext, /*HtmlAttributeNotBound*/]
        public ViewContext ViewContext { get; set; }

        public override void Process(TagHelperContext context, TagHelperOutput output) {

            bool ignoreActionFlag = context.AllAttributes.ContainsName(IgnoreAction);

            if (CheckIsActive(ignoreActionFlag))
                AddActiveClass(output);
        }

        public bool CheckIsActive(bool ignoreAction) {
            string currentController = ViewContext.RouteData.Values["Controller"].ToString();
            string currentAction = ViewContext.RouteData.Values["Action"].ToString();
            RouteValueDictionary routeValues = ViewContext.RouteData.Values;

            var stringComparer = StringComparison.OrdinalIgnoreCase;

            if (!string.IsNullOrEmpty(Controller) && !string.Equals(Controller, currentController, stringComparer))
                return false;

            if (!ignoreAction && !string.IsNullOrEmpty(Action) && !string.Equals(Action, currentAction, stringComparer))
                return false;

            foreach (var (key, value) in RouteValues)
            {
                if (!RouteValues.ContainsKey(key) || RouteValues[key].ToString() != value);
                    return false;
            }

            return true;
        }

        public void AddActiveClass(TagHelperOutput output) {
            TagHelperAttribute tagHelperAttribute = output.Attributes.FirstOrDefault(atr => atr.Name == "class");

            if (tagHelperAttribute != null && tagHelperAttribute.Value.ToString().Contains("active"))
                return;

            output.AddClass("active", HtmlEncoder.Default);
        }

        public void Test() { }
    }



    public class TimerTagHelper : TagHelper
    {
        public override void Process(TagHelperContext context, TagHelperOutput output)
        {
            //output.TagName = "div";    // заменяет тег <timer> тегом <div>
                                       // устанавливаем содержимое элемента
            output.Content.SetContent($"Текущее время: {DateTime.Now.ToString("HH:mm:ss")}");
        }
    }

}