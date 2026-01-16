using System.Diagnostics;
using System.Numerics;
using RoboScapeSimulator.Entities;
using RoboScapeSimulator.Entities.Robots;
using RoboScapeSimulator.Environments.Helpers;
using RoboScapeSimulator.IoTScape.Devices;

namespace RoboScapeSimulator.Environments
{
    class BoxPushingRace : EnvironmentConfiguration
    {
        readonly bool _lidar = false;

        public BoxPushingRace(bool lidar = false)
        {
            Name = $"Box Pushing Race {(lidar ? "with lidar" : "")}";
            ID = $"boxpushrace{(lidar ? "_lidar" : "")}";
            Description = "Box pushing race environment";
            _lidar = lidar;
        }

        public override object Clone()
        {
            return new BoxPushingRace(_lidar);
        }

        public override void Setup(Room room)
        {
            Trace.WriteLine($"Setting up {Name} environment");

            StopwatchTimer timer = new(room);

            // Ground
            _ = new Ground(room);

            // Table
            _ = new Cube(room, 4f, 1, 4f, new Vector3(0, 0.5f, 0), Quaternion.Identity, true, nameOverride: "table");

            // Demo robots
            Random rng = new();

            Trigger trigger = new Trigger(room, new Vector3(0, 0.125f, 0), Quaternion.Identity, 10, 0.25f, 10);

            var robot = new ParallaxRobot(room, rng.PointOnCircle(0.25f, 1.45f));

            PositionSensor locationSensor = new(robot);
            locationSensor.Setup(room);

            if (_lidar)
            {
                var lidar = new LIDARSensor(robot) { Offset = new(0, 0.25f, 0.07f), NumRays = 15, StartAngle = MathF.PI / 2, AngleRange = MathF.PI, MaxDistance = 5 };
                lidar.Setup(room);
            }


            // Cubes
            int boxes = 5;
            foreach (var point in Utils.PointsOnCircle(boxes, 1.5f, 1.45f))
            {
                Console.WriteLine(point);
                _ = new Cube(room, 0.5f, 0.5f, 0.5f, point, Quaternion.Identity, visualInfo: new VisualInfo() { Color = "#B85" });
            }


            trigger.OnTriggerEnter += (o, e) =>
            {
                if (trigger.InTrigger.Count(e => e is Cube) > boxes)
                {
                    timer.Stop();
                }
            };

            room.OnReset += (o, e) =>
            {
                trigger.Reset();
                timer.Reset();
                timer.Start();
            };
        }
    }
}