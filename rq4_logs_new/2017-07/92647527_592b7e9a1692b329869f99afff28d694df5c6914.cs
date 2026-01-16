using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.DependencyInjection;

namespace MAF.FeaturesFlipping.AspNetCore
{
    public class FeatureFilterAttribute : Attribute, IFilterFactory
    {
        public string Application { get; }
        public string Scope { get; }
        public string Feature { get; }

        public FeatureFilterAttribute(string application, string scope, string feature)
        {
            Application = application;
            Scope = scope;
            Feature = feature;
        }

        public IFilterMetadata CreateInstance(IServiceProvider serviceProvider)
        {
            var featureService = serviceProvider.GetRequiredService<IFeatureService>();
            return new FeatureResourceFilter(featureService, new FeatureName(Application, Scope, Feature));
        }

        public bool IsReusable => false;

        private class FeatureResourceFilter : IAsyncResourceFilter
        {
            private readonly IFeatureService _featureService;
            private readonly IFeatureName _featureName;

            public FeatureResourceFilter(IFeatureService featureService, IFeatureName featureName)
            {
                _featureService = featureService ?? throw new ArgumentNullException(nameof(featureService));
                _featureName = featureName ?? throw new ArgumentNullException(nameof(featureName));
            }

            public async Task OnResourceExecutionAsync(ResourceExecutingContext context, ResourceExecutionDelegate next)
            {
                if (!await _featureService.IsFeatureActiveAsync(_featureName))
                {
                    context.Result = new NotFoundResult();
                }
                else
                {
                    await next();
                }
            }
        }
    }
}