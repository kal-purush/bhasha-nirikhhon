using dbcontext;
using dbcontext.Classes;
using Microsoft.AspNetCore.Mvc.Filters;

namespace Linktindr_be.Filters
{
    public class AuthenticationFilter : IActionFilter
    {
        private readonly OurContext OC;

        public AuthenticationFilter(OurContext OC)
        {
            this.OC = OC;
        }

        public void OnActionExecuting(ActionExecutingContext context)
        {
            var requestHeaders = context.HttpContext.Request.Headers;
            var tokenHeader = requestHeaders["Authorization"];

            if (tokenHeader.Count > 0)
            {
                string? TokenAsString = tokenHeader[0];
                if (TokenAsString != null)
                {
                    // Look for token for medewerker
                    Medewerker? m = OC.Medewerker.FirstOrDefault(m => m.Token.Equals(TokenAsString));
                    if (m != null)
                    {
                        context.HttpContext.Items.Add("User", m);
                    }

                    TalentManager? t = OC.TalentManager.FirstOrDefault(m => m.Token.Equals(TokenAsString));
                    if (t != null)
                    {
                        context.HttpContext.Items.Add("User", t);
                    }

                    Opdrachtgever? o = OC.Opdrachtgever.FirstOrDefault(m => m.Token.Equals(TokenAsString));
                    if (o != null)
                    {
                        context.HttpContext.Items.Add("User", o);
                    }
                }
            }

        }

        public void OnActionExecuted(ActionExecutedContext context)
        {
            // our code after action executes
        }
    }
}