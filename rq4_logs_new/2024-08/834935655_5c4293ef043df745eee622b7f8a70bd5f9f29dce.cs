using Moq;
using ProjectE.Application.Commands.Projects.CreateProject;
using ProjectE.Core.Entities;
using ProjectE.Core.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProjectE.UnitTests.Application.Commands
{
    public class CreateProjectCommandHandlerTest
    {
        public Guid customer = Guid.NewGuid();
        public Guid freelancer = Guid.NewGuid();

        [Fact]
        public async Task InputDataIsOk_ReturnProjectId()
        {
            var projectRepositoryMock = new Mock<IProjectRepository>();

            var createProjectCommand = new CreateProjectCommand
            {
                Title = "Test",
                Description = "Description Test",
                CustomerId = customer,
                FreelancerId = freelancer
            };

            var createProjectHandler = new CreateProjectHandler(projectRepositoryMock.Object);

            var id = await createProjectHandler.Handle(createProjectCommand, new CancellationToken());

            Assert.NotNull(id);
            projectRepositoryMock.Verify(x => x.CreateProjectAsync(It.IsAny<Project>()), Times.Once);
        }
    }
}