using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DFC.App.MatchSkills.Application.ServiceTaxonomy;
using DFC.App.MatchSkills.Application.Session.Interfaces;
using DFC.App.MatchSkills.Application.Session.Models;
using DFC.App.MatchSkills.Controllers;
using DFC.App.MatchSkills.Interfaces;
using DFC.App.MatchSkills.Models;
using DFC.App.MatchSkills.Service;
using DFC.App.MatchSkills.Services.ServiceTaxonomy;
using DFC.App.MatchSkills.Services.ServiceTaxonomy.Models;
using DFC.App.MatchSkills.Test.Helpers;
using DFC.App.MatchSkills.ViewModels;
using DFC.Personalisation.Common.Net.RestClient;
using DFC.Personalisation.Domain.Models;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ReturnsExtensions;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Unit.Controllers
{
    public class RelatedSkillsControllerTests
    {
        private IOptions<CompositeSettings> _compositeSettings;
        private ISessionService _sessionService;
        private ICookieService _cookieService;
        private IServiceTaxonomySearcher _serviceTaxonomySearcher;
        private IOptions<ServiceTaxonomySettings> _settings;
        const string _skillsJson = "{  " +
                                      "\"skills\": [" +
                                      "{    \"skillType\": \"knowledge\",    \"skill\": \"toxicology\",    \"lastModified\": \"2016-12-20T19:32:45Z\",    \"alternativeLabels\": [\"study of toxicity\", \"chemical toxicity\", \"study of adverse effects of chemicals\", \"studies of toxicity\"],    \"uri\": \"http:\\/\\/data.europa.eu\\/esco\\/skill\\/b70ab677-5781-40b5-9198-d98f4a34310f\",    \"matches\": {      \"hiddenLabels\": []," + "      \"skill\": [\"toxicology\"],      \"alternativeLabels\": [\"study of toxicity\", \"chemical toxicity\", \"studies of toxicity\"]    },    \"skillReusability\": \"cross-sectoral\"  }, " +
                                      "{    \"skillType\": \"competency\",    \"skill\": \"perform toxicological studies\",    \"lastModified\": \"2016-12-20T19:37:05Z\",    \"alternativeLabels\": [\"apply toxicological testing methods\", \"perform toxicological tests\", \"perform toxicological study\", \"carry out toxicological studies\"],    \"uri\": \"http:\\/\\/data.europa.eu\\/esco\\/skill\\/000bb1e4-89f0-4b86-be05-05ece3641724\",    \"matches\": {      \"hiddenLabels\": [],      \"skill\": [\"perform toxicological studies\"],      \"alternativeLabels\": [\"apply toxicological testing methods\", \"perform toxicological tests\", \"perform toxicological study\", \"carry out toxicological studies\"]    },    \"skillReusability\": \"cross-sectoral\"  }, " +
                                      "{    \"skillType\": \"knowledge\",    \"skill\": \"food toxicity\",    \"lastModified\": \"2016-12-20T19:05:31Z\",    \"alternativeLabels\": [\"food spoilage\", \"prevention of food poisoning\", \"toxicity of foods\", \"food poisoning\", \"the  toxicity of food\"],    \"uri\": \"http:\\/\\/data.europa.eu\\/esco\\/skill\\/4e081e0a-e25f-4f6e-9c75-e9043ba08aad\",    \"matches\": {      \"hiddenLabels\": [],      \"skill\": [\"food toxicity\"],      \"alternativeLabels\": [\"toxicity of foods\", \"the  toxicity of food\"]    },    \"skillReusability\": \"sector-specific\"  }]}";

        [SetUp]
        public void Init()
        {
            _settings = Options.Create(new ServiceTaxonomySettings());
            _settings.Value.ApiUrl = "https://dev.api.nationalcareersservice.org.uk/servicetaxonomy";
            _settings.Value.ApiKey = "mykeydoesnotmatterasitwillbemocked";
            _settings.Value.SearchOccupationInAltLabels = "true";

            _sessionService = Substitute.For<ISessionService>();
            _compositeSettings = Options.Create(new CompositeSettings());

            var handlerMock = MockHelpers.GetMockMessageHandler(_skillsJson);
            var restClient = new RestClient(handlerMock.Object);
            _serviceTaxonomySearcher = new ServiceTaxonomyRepository(restClient);
            _cookieService = Substitute.For<ICookieService>();
            _sessionService.GetUserSession(Arg.Any<string>()).ReturnsForAnyArgs(new UserSession());
            _cookieService.TryGetPrimaryKey(Arg.Any<HttpRequest>(), Arg.Any<HttpResponse>())
                .ReturnsForAnyArgs("This is My Value");
        }

        [Test]
        public async Task WhenBodyCalled_ReturnHtml()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            var result = await controller.Body() as ViewResult;
            result.Should().NotBeNull();
            result.Should().BeOfType<ViewResult>();
            result.ViewName.Should().BeNull();
        }

        [Test]
        public async Task WhenGetBodyWithBlankInputCalled_ReturnHtml()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            var result = await controller.Body("") as ViewResult;
            var model = result.Model as RelatedSkillsCompositeViewModel;
            model.RelatedSkills.Should().BeNullOrEmpty();
            result.Should().NotBeNull();
            result.Should().BeOfType<ViewResult>();
        }
        [Test]
        public async Task WhenGetBodyWithInputCalled_ReturnDataAndHtml()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            var result = await controller.Body("Test") as ViewResult;
            var model = result.Model as RelatedSkillsCompositeViewModel;
            model.RelatedSkills.Should().NotBeNullOrEmpty();
            result.Should().NotBeNull();
            result.Should().BeOfType<ViewResult>();


        }
        [Test]
        public async Task WhenPostBodyHasNoValues_FlagError_ReturnHtml()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            var formCollection = Substitute.For<IFormCollection>();
            formCollection.Keys.Count.ReturnsForAnyArgs(0);
            var result = await controller.Body(formCollection, String.Empty) as ViewResult;
            var model = result.Model as RelatedSkillsCompositeViewModel;
            model.HasError.Should().BeTrue();
            result.Should().NotBeNull();
            result.Should().BeOfType<ViewResult>();
            result.ViewName.Should().BeNull();
        }
        [Test]
        public async Task WhenPostBodyHasOneValue_FlagError_ReturnHtml()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            var formCollection = Substitute.For<IFormCollection>();
            formCollection.Keys.Count.ReturnsForAnyArgs(1);
            var result = await controller.Body(formCollection, String.Empty) as ViewResult;
            var model = result.Model as RelatedSkillsCompositeViewModel;
            model.HasError.Should().BeTrue();
            result.Should().NotBeNull();
            result.Should().BeOfType<ViewResult>();
            result.ViewName.Should().BeNull();
        }

        [Test]
        public async Task When_AddSkillsWithSearchTerm_Then_ShouldAddSelectedSkills()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            controller.HttpContext.Request.QueryString = QueryString.Create(CookieService.CookieName, "Abc123");
            controller.ControllerContext.HttpContext = MockHelpers.SetupControllerHttpContext().Object;

            var dic = new System.Collections.Generic.Dictionary<string, Microsoft.Extensions.Primitives.StringValues>();
            dic.Add("somekey--somevalue", "key1");
            dic.Add("searchTerm", "");
            dic.Add("somekey1--somevalue1", "key2");
            var collection = new Microsoft.AspNetCore.Http.FormCollection(dic);


            _sessionService.GetUserSession(Arg.Any<string>()).ReturnsForAnyArgs(MockHelpers.GetUserSession(true));
            _sessionService.UpdateUserSessionAsync(Arg.Any<UserSession>()).ReturnsNullForAnyArgs();

            var result = await controller.Body(collection, "SearchTerm") as RedirectResult;

            result.Should().NotBeNull();
            result.Should().BeOfType<RedirectResult>();
            result.Url.Should().Be($"/{CompositeViewModel.PageId.SkillsBasket}");
        }
        [Test]
        public async Task When_AddSkillsWithoutSearchTerm_Then_ShouldAddSelectedSkills()
        {
            var controller = new RelatedSkillsController(_serviceTaxonomySearcher, _settings, _compositeSettings, _sessionService, _cookieService);
            controller.ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            };

            controller.HttpContext.Request.QueryString = QueryString.Create(CookieService.CookieName, "Abc123");
            controller.ControllerContext.HttpContext = MockHelpers.SetupControllerHttpContext().Object;

            var dic = new System.Collections.Generic.Dictionary<string, Microsoft.Extensions.Primitives.StringValues>();
            dic.Add("somekey--somevalue", "key1");
            dic.Add("somekey1--somevalue1", "key2");
            var collection = new Microsoft.AspNetCore.Http.FormCollection(dic);


            _sessionService.GetUserSession(Arg.Any<string>()).ReturnsForAnyArgs(MockHelpers.GetUserSession(true));
            _sessionService.UpdateUserSessionAsync(Arg.Any<UserSession>()).ReturnsNullForAnyArgs();

            var result = await controller.Body(collection, "") as RedirectResult;

            result.Should().NotBeNull();
            result.Should().BeOfType<RedirectResult>();
            result.Url.Should().Be($"/{CompositeViewModel.PageId.SkillsBasket}");
        }
        [Test]
        public void WhenRelatedSkillsControllerInvoked_ThenModelPropertiesCanBeSetAndRetrieved()
        {
            var model = new RelatedSkillsCompositeViewModel()
            {
                HasError = true,
                CompositeSettings = _compositeSettings.Value,
                Skills = { new Skill("id", "name") },
                SearchTerm = "SearchTerm",
                RelatedSkills = { new Skill("id", "secondName")}
            };
            var hasError = model.HasError;
            var settings = model.CompositeSettings;
            var skills = model.Skills;
            var searchTerm = model.SearchTerm;
            var relatedSkills = model.RelatedSkills;
        }
    }
}