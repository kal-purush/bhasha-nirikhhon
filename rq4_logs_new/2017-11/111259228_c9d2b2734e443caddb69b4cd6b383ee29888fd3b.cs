using Ploeh.AutoFixture;
using Ploeh.AutoFixture.AutoFakeItEasy;
using Ploeh.AutoFixture.Xunit2;
using System;

namespace WebApp.Tests
{
    public class AutoFakeItEasyDataAttribute : AutoDataAttribute
    {
        public AutoFakeItEasyDataAttribute(params Type[] customizationTypes)
            : base(new Fixture())
        {
            Fixture.Customize(new AutoFakeItEasyCustomization());
            foreach (var customizationType in customizationTypes)
                Fixture.Customize((ICustomization)Activator.CreateInstance(customizationType));
        }
    }
}