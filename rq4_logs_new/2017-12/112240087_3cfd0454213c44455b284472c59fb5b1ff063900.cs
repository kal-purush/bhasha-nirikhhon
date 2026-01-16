using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using fhirStartersApp.Data;
using fhirStartersApp.DecisionSupport;
using fhirStartersApp.DecisionSupport.MtbiRules;
using Xunit;

namespace fhirStartersApp.Test
{
    public class DischargeRuleTest
    {
        private readonly DischargeRule _rule;
        private readonly Patient _patient;

        public DischargeRuleTest()
        {
            _rule = new DischargeRule();
            _patient = new Patient
            {
                Data = new Observations()
            };
        }

        [Fact]
        public async Task RequestRestRecommendedGivenNoDataForRestRecommended()
        {
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.MoreInformationRequired &&
                     x.Description == nameof(Observations.RestRecommended));
        }

        [Fact]
        public async Task RequestNumberDaysRecommendedGivenRestRecommended()
        {
            _patient.Data.RestRecommended = true;
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.MoreInformationRequired &&
                     x.Description == nameof(Observations.RestRecommendedDays));
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(14)]
        public async Task ManagementPlanHasDaysRestRecommendedGivenRestRecommended(int daysRestRecommended)
        {
            _patient.Data.RestRecommended = true;
            _patient.Data.RestRecommendedDays = daysRestRecommended;
            var results = await _rule.EvaluateAsync(_patient);

            var expected = string.Format(DischargeRule.DaysRestTemplate, daysRestRecommended);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.ManagementPlanRecommendation &&
                     x.Description == expected);
        }

        [Fact]
        public async Task DrugsRecommendedGivenHeadache()
        {
            _patient.Data.SevereHeadache = Trilean.Yes;
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.ManagementPlanRecommendation &&
                     x.Description == DischargeRule.HeadacheDrugs);
        }

        [Fact]
        public async Task SunglassesEarplugsHeadphonesGivenBotheredByNoiseOrLight()
        {
            _patient.Data.LightNoiseSensitivity = Trilean.Yes;
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.ManagementPlanRecommendation &&
                     x.Description == DischargeRule.SunglassesEarplugs);
        }

        [Fact]
        public async Task RequestIncludeSchoolRecommendationsGivenNullIncludeSchoolRecommendations()
        {
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.MoreInformationRequired &&
                     x.Description == nameof(Observations.IncludeSchoolRecommendations));
        }

        [Fact]
        public async Task SchoolRecommendationsGivenIncludeSchoolRecommendations()
        {
            _patient.Data.IncludeSchoolRecommendations= true;
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.ManagementPlanRecommendation &&
                     x.Description == DischargeRule.SchoolRecommendations);
        }

        [Fact]
        public async Task OtherRecommendationsIncludedGivenOtherRecommendations()
        {
            const string customDischargeInstructions = "These are custom discharge instructions.\r\nThey should be followed to the 'T'.\r\n◦ Take good naps. \r\n◦ Rest enough time.\r\n\r\nGo back to sports eventually.";
            _patient.Data.CustomDischargeInstructions = customDischargeInstructions;
            var results = await _rule.EvaluateAsync(_patient);
            Assert.Contains(results,
                x => x.Type == DecisionSupportResultType.ManagementPlanRecommendation &&
                     x.Description == customDischargeInstructions);
        }
    }
}