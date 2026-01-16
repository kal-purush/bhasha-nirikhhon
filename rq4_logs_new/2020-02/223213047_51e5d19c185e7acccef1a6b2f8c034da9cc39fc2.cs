using System;
using System.Collections.Generic;
using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Unit.Controllers
{
    public class OccupationSearchResultsController
    {
        [Test]
        public void WhenCalled_Controller_Created()
        {
            // Arrange
            var controller = new OccupationSearchResultsController();

            // Assert
            controller.Should().NotBeNull();
        }
    }
}