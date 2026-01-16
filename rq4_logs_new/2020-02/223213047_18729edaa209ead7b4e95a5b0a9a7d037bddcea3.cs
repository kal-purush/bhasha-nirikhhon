using System;
using System.Collections.Generic;
using System.Text;
using DFC.App.MatchSkills.Application.Session.Interfaces;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.App.MatchSkills.Controllers;
using DFC.App.MatchSkills.Interfaces;
using DFC.App.MatchSkills.Models;
using DFC.App.MatchSkills.Service;
using DFC.App.MatchSkills.Services.ServiceTaxonomy;
using DFC.App.MatchSkills.Services.ServiceTaxonomy.Models;
using DFC.App.MatchSkills.Test.Helpers;
using DFC.Personalisation.Common.Net.RestClient;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Unit.Controllers
{
    public class OccupationSearchResultsControllerTests
    {
        private IOptions<ServiceTaxonomySettings> _settings;
        private IOptions<CompositeSettings> _compositeSettings;
        private ServiceTaxonomyRepository _serviceTaxonomyRepository;
        private const string Path = "OccupationSearch";
        private ISessionService _sessionService;
        private ICookieService _cookieService;
        const string SkillsJson ="{\"occupations\": [{\"uri\": \"http://data.europa.eu/esco/occupation/114e1eff-215e-47df-8e10-45a5b72f8197\",\"occupation\": \"renewable energy consultant\",\"alternativeLabels\": [\"alt 1\"],\"lastModified\": \"03-12-2019 00:00:01\"}]}";           
        
        [SetUp]
        public void Init()
        {
            _settings = Options.Create(new ServiceTaxonomySettings());
            _settings.Value.ApiUrl = "https://dev.api.nationalcareersservice.org.uk/servicetaxonomy";
            _settings.Value.ApiKey = "mykeydoesnotmatterasitwillbemocked";
            _settings.Value.SearchOccupationInAltLabels ="true";
            _sessionService = Substitute.For<ISessionService>();
            _compositeSettings = Options.Create(new CompositeSettings());

            
            var handlerMock = MockHelpers.GetMockMessageHandler(SkillsJson);
            var restClient = new RestClient(handlerMock.Object);
            _serviceTaxonomyRepository = new ServiceTaxonomyRepository(restClient);
            _sessionService.GetUserSession(Arg.Any<string>()).ReturnsForAnyArgs(new UserSession());
            _cookieService = new CookieService(new EphemeralDataProtectionProvider());

        }
        [Test]
        public void WhenCalled_Controller_Created()
        {
            
            // Arrange
            var controller = new OccupationSearchResultsController(_compositeSettings, _sessionService, _cookieService);

            // Assert
            controller.Should().NotBeNull();
        }
    }
}