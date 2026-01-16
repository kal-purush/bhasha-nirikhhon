using AutomatedCar.Models;
using Xunit;

namespace AutomatedCarTests.Models
{
    public class SteeringTests
    {
        [Theory]
        [InlineData(10, 10, "WorldObjects/car_1_blue.png", 2, 2)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 20, 20)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 100, 100)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 101, 100)]
        public void SteeringWheelRotationRight(int x, int y, string filename, int rotation, int expected)
        {
            AutomatedCar.Models.AutomatedCar AutCar = new AutomatedCar.Models.AutomatedCar(x, y, filename);
            for (int i = 0; i < rotation; i++)
            {
                AutCar.RotateSteeringWheelRight();
            }
            Assert.Equal(expected, AutCar.wheelPosition);
        }

        [Theory]
        [InlineData(10, 10, "WorldObjects/car_1_blue.png", 2, -2)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 20, -20)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 100, -100)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 101, -100)]
        public void SteeringWheelRotationLeft(int x, int y, string filename, int rotation, int expected)
        {
            AutomatedCar.Models.AutomatedCar AutCar = new AutomatedCar.Models.AutomatedCar(x, y, filename);
            for (int i = 0; i < rotation; i++)
            {
                AutCar.RotateSteeringWheelLeft();
            }
            Assert.Equal(expected, AutCar.wheelPosition);
        }

        [Theory]
        [InlineData(10, 10, "WorldObjects/car_1_blue.png", 2, 2)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 20, 20)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 100, 100)]
        [InlineData(200, 400, "WorldObjects/car_1_blue.png", 101, 100)]
        public void SteeringWheelRelease(int x, int y, string filename, int rotation, int expected)
        {
            AutomatedCar.Models.AutomatedCar AutCar = new AutomatedCar.Models.AutomatedCar(x, y, filename);
            for (int i = 0; i < rotation; i++)
            {
                AutCar.RotateSteeringWheelLeft();
            }
            for (int i = 0; i < rotation; i++)
            {
                AutCar.ReleaseSteeringWheel();
            }
            Assert.Equal(0, AutCar.wheelPosition);
        }
    }
}