using Moq;
using MyCalendar.Mobile.Common.Services.Appointment;
using MyCalendar.Mobile.Views.Home.TabPages.AddAppointment;
using Prism.Navigation;
using Prism.Services;
using System;
using Xunit;

namespace MyCalendar.Mobile.Test.ViewModels
{
    public class AddAppointmentPageViewModelTests
    {
        private AddAppointmentPageViewModel viewModel;

        private Mock<IAppointmentService> appointmentServiceMock;
        private Mock<IPageDialogService> dialogServiceMock;

        public AddAppointmentPageViewModelTests()
        {
            this.appointmentServiceMock = new Mock<IAppointmentService>();
            this.dialogServiceMock = new Mock<IPageDialogService>();
            var navigationServiceMock = new Mock<INavigationService>();

            this.viewModel = new AddAppointmentPageViewModel(
                this.appointmentServiceMock.Object,
                this.dialogServiceMock.Object,
                navigationServiceMock.Object);
        }

        [Fact]
        public void Should_Allow_Entering_Title()
        {
            // Arrange
            var title = "Test Title";

            // Act
            this.viewModel.Title = title;

            // Assert
            Assert.Equal(title, this.viewModel.Title);
        }

        [Fact]
        public void Should_Allow_Entering_Location()
        {
            // Arrange
            var location = "Test Location";

            // Act
            this.viewModel.Location = location;

            // Assert
            Assert.Equal(location, this.viewModel.Location);
        }

        [Fact]
        public void Should_Allow_Entering_StartTime_And_EndTime()
        {
            // Arrange
            var startTime = DateTime.Now;
            var endTime = startTime.AddHours(10);

            // Act
            this.viewModel.StartTime = startTime;
            this.viewModel.EndTime = endTime;

            // Assert
            Assert.Equal(startTime, this.viewModel.StartTime);
            Assert.Equal(endTime, this.viewModel.EndTime);
        }

        [Fact]
        public void Should_Allow_Setting_AllDay()
        {
            // Arrange
            var allDay = true;

            // Act
            this.viewModel.AllDay = allDay;

            // Assert
            Assert.Equal(allDay, this.viewModel.AllDay);
        }

        [Fact]
        public void Should_Create_Appointment_On_Submit_Button()
        {
            // Arrange
            var title = "Test Title";
            var location = "Test Location";
            var allDay = false;
            var startTime = DateTime.Now;
            var endTime = startTime.AddHours(10);

            var appointment = new AppointmentViewModel
            {
                Title = title,
                Location = location,
                AllDay = allDay,
                StartTime = startTime,
                EndTime = endTime,
            };

            // Act
            this.viewModel.Title = title;
            this.viewModel.Location = location;
            this.viewModel.StartTime = startTime;
            this.viewModel.EndTime = endTime;
            this.viewModel.AllDay = allDay;

            this.viewModel.SubmitCommand.Execute(null);

            // Assert
            this.appointmentServiceMock.Verify(m => m.CreateAppointmentAsync(appointment), Times.Once());
        }

        [Fact]
        public void Should_Handle_Exception_On_Creating_Appointment()
        {
            // Arrange
            var ex = new Exception();

            this.appointmentServiceMock
                .Setup(m => m.CreateAppointmentAsync(It.IsAny<AppointmentViewModel>()))
                .ThrowsAsync(ex);

            // Act
            this.viewModel.SubmitCommand.Execute(null);

            // Assert
            this.dialogServiceMock.Verify(m => m.DisplayAlertAsync("Could not create appointment", "Error on creating appointment: " + ex.Message, "OK"), Times.Once());
        }
    }
}