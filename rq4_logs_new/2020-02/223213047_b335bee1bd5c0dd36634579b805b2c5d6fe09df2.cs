using System;
using System.Collections.Generic;
using System.Text;
using DFC.App.MatchSkills.ViewModels;
using FluentAssertions;
using NUnit.Framework;

namespace DFC.App.MatchSkills.Test.Unit.ViewModels
{
    class OccupationSearchResultsCompositeViewModelTests
    {
        [Test]
        public void When_OccupationSet_Then_ToReturnOccupation()
        {
            // Arrange.
            var vm = new OccupationSearchResultsCompositeViewModel();
            vm.HasError = true;
            // Assert.
            vm.HasError.Should().Be(true);
        }
    }
}