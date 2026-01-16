using AutoFixture;
using CautionaryAlertsApi.V1.Boundary.Request;
using CautionaryAlertsApi.V1.Infrastructure;
using System;

namespace CautionaryAlertsApi.Tests
{
    public static class CreateCautionaryAlertFixture
    {
        public static CreateCautionaryAlert GenerateValidCreateCautionaryAlertFixture(string defaultString, Fixture fixture)
        {
            var alert = fixture.Build<Alert>()
                .With(x => x.Code, defaultString[..CreateCautionaryAlertConstants.ALERTCODELENGTH])
                .With(x => x.Description, defaultString[..CreateCautionaryAlertConstants.ALERTDESCRIPTION])
                .Create();

            var assetDetails = fixture.Build<AssetDetails>()
                .With(x => x.FullAddress, defaultString[..CreateCautionaryAlertConstants.FULLADDRESSLENGTH])
                .With(x => x.PropertyReference, defaultString[..CreateCautionaryAlertConstants.PROPERTYREFERENCELENGTH])
                .With(x => x.UPRN, defaultString[..CreateCautionaryAlertConstants.UPRNLENGTH])
                .Create();

            var personDetails = fixture.Build<PersonDetails>()
                .With(x => x.Name, defaultString[..CreateCautionaryAlertConstants.PERSONNAMELENGTH])
                .Create();

            var cautionaryAlert = fixture.Build<CreateCautionaryAlert>()
                .With(x => x.Alert, alert)
                .With(x => x.PersonDetails, personDetails)
                .With(x => x.AssetDetails, assetDetails)
                .With(x => x.IncidentDescription, defaultString[..CreateCautionaryAlertConstants.INCIDENTDESCRIPTIONLENGTH])
                .With(x => x.IncidentDate, fixture.Create<DateTime>().AddDays(-1))
                .With(x => x.AssureReference, defaultString[..CreateCautionaryAlertConstants.ASSUREREFERENCELENGTH])
                .Create();

            return cautionaryAlert;
        }
    }
}