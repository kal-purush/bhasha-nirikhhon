using AutoFixture;
using Bogus;
using FluentAssertions;
using OfficeReview.Domain.Profile.Enums;
using OfficeReview.Domain.Profile.Root;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace OfficePerformanceReview.DomainTest.ProfileTest
{
    public class StaffTests
    {
        private Fixture _fixture;
        private Faker _faker;

        [SetUp]
        public void Setup()
        {
            _fixture = new Fixture();
            _faker = new Faker();
        }

        [Test]
        public void init_should_be_successed()
        {
            // Arrange
            var staffFake = TestHelpers.GetStaffParam();

            // Act
            var staff = new Staff(staffFake.Name, staffFake.Position, staffFake.Email, staffFake.Roles, staffFake.Team);

            // Assert
            TestHelpers.AssertValidGuid(staff.StaffGuid);
            TestHelpers.AssertNotNullOrEmpty(staff.Name, nameof(staff.Name));
            TestHelpers.AssertNotNullOrEmpty(staff.PositionTitle, nameof(staff.PositionTitle));
            TestHelpers.IsValidEmail(staff.Email).Should().BeTrue();
            staff.Roles.Should().Equal(staffFake.Roles);
            staff.Team.Should().Be(staffFake.Team);
            staff.IsActive.Should().BeTrue();
            staff.IsDeleted.Should().BeFalse();
        }

        [Test]
        public void init_should_throw_argument_exception_when_email_isinvalid()
        {
            // Arrange
            var staffFake = TestHelpers.GetStaffParam();

            staffFake.Email = "invalid_email";

            // Act
            Action act = () => new Staff(staffFake.Name, staffFake.Position, staffFake.Email, staffFake.Roles, staffFake.Team);

            // Assert
            act.Should().Throw<ArgumentException>().WithMessage("*Invalid email*");
        }

        [Test]
        public void setstaff_should_be_successed()
        {
            // Arrange
            var staff = TestHelpers.GetStaff();

            var newStaff = TestHelpers.GetStaffParam();
            // Act
            staff.SetStaff(newStaff.Name, newStaff.Position, newStaff.Roles, newStaff.Team);

            // Assert
            TestHelpers.AssertNotNullOrEmpty(staff.Name, nameof(staff.Name));
            TestHelpers.AssertNotNullOrEmpty(staff.PositionTitle, nameof(staff.PositionTitle));
            staff.Roles.Should().Equal(newStaff.Roles);
            staff.Team.Should().Be(newStaff.Team);
        }

        [Test]
        public void setdelete_should_be_successed()
        {
            // Arrange
            var staff = TestHelpers.GetStaff();
            // Act
            staff.SetDelete();

            // Assert
            staff.IsDeleted.Should().BeTrue();
        }

        [Test]
        public void SetDeActivate_ShouldSetIsActiveToFalse()
        {
            // Arrange
            var staff = TestHelpers.GetStaff();

            // Act
            staff.SetDeActivate();

            // Assert
            staff.IsActive.Should().BeFalse();
        }

        [Test]
        public void ChangeTeam_ShouldUpdateTeamProperty()
        {
            // Arrange
            var staff = TestHelpers.GetStaff();
            var newTeam = _fixture.Create<Team>();

            // Act
            staff.ChangeTeam(newTeam);

            // Assert
            staff.Team.Should().Be(newTeam);
        }

        [Test]
        public void SetChangeRole_ShouldAddNewRoleToRolesList()
        {
            // Arrange
            var staff = TestHelpers.GetStaff();
            var newRole = _fixture.Create<Role>();

            // Act
            staff.SetChangeRole(newRole);

            // Assert
            staff.Roles.Should().Contain(newRole);
        }

       
    }
}


