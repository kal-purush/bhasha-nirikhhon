using System.CodeDom;
using TechTalk.SpecFlow.Generator;
using TechTalk.SpecFlow.Generator.UnitTestProvider;
using TechTalk.SpecFlow.Utils;

namespace FeatureBaseClassGenerator.SpecFlowPlugin
{
    public class FeatureBaseClassGeneratorProvider : NUnit3TestGeneratorProvider
    {
        private const string FEATURE_BASE_CLASS = "FeatureBase";

        public FeatureBaseClassGeneratorProvider(CodeDomHelper codeDomHelper) : base(codeDomHelper)
        {
        }

        public override void FinalizeTestClass(TestClassGenerationContext generationContext)
        {
            base.FinalizeTestClass(generationContext);
            var featureBaseClassTypeReference = new CodeTypeReference(FEATURE_BASE_CLASS);
            generationContext.TestClass.BaseTypes.Add(featureBaseClassTypeReference);
        }
    }
}