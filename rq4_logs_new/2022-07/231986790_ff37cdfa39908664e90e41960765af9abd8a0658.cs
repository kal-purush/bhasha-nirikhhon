using System.Diagnostics;
using System.Numerics;
using RoboScapeSimulator.Entities;
using RoboScapeSimulator.Entities.Robots;
using RoboScapeSimulator.Environments.Helpers;
using RoboScapeSimulator.IoTScape;
using RoboScapeSimulator.IoTScape.Devices;
namespace RoboScapeSimulator.Environments
{
    internal class TreasureHuntEnvironment : EnvironmentConfiguration
    {
        public TreasureHuntEnvironment()
        {
            Name = "Treasure Hunt";
            ID = "treasurehunt";
            Description = "treasureSensor Demo Environment";
        }

        private IoTScapeObject? treasureSensor;
        private PositionSensor? locationSensor;

        public override object Clone()
        {
            return new TreasureHuntEnvironment();
        }

        public override void Setup(Room room)
        {
            Trace.WriteLine($"Setting up {Name} environment");

            Random rng = new();

            // Ground
            Ground ground = new(room);

            // Walls
            EnvironmentUtils.MakeWalls(room);

            // Demo robot 
            ParallaxRobot robot = new(room, new Vector3(0, 0.25f, 0), Quaternion.Identity);

            Cube chest = new(room, 0.2f, 0.5f, 0.2f, initialPosition: rng.PointOnAnnulus(1.5f, 3f, -150f), initialOrientation: Quaternion.Identity, visualInfo: new VisualInfo() { ModelName = "chest_opt.glb", ModelScale = 0.4f }, isKinematic: true);
            Trigger chestTrigger = new(room, new Vector3(chest.Position.X, 0, chest.Position.Z), Quaternion.Identity, 0.2f, 2f, 0.2f);

            chestTrigger.OnTriggerEnter += (o, e) =>
            {
                if (e == robot)
                {
                    // Reveal chest
                    chest.Position = new Vector3(chest.Position.X, 0, chest.Position.Z);
                }
            };

            locationSensor = new(robot);
            locationSensor.Setup(room);

            IoTScapeServiceDefinition treasureSensorDefinition = new(
                "MetalDetector",
                new IoTScapeServiceDescription() { version = "1" },
                "",
                new Dictionary<string, IoTScapeMethodDescription>()
                {
                {"getIntensity", new IoTScapeMethodDescription(){
                    documentation = "Get metal detector reading at current location",
                    paramsList = new List<IoTScapeMethodParams>(){},
                    returns = new IoTScapeMethodReturns(){type = new List<string>(){
                        "number"
                    }}
                }}
                },
                new Dictionary<string, IoTScapeEventDescription>());

            var sensorBody = robot;
            var targetBody = chestTrigger;
            int targetIntensity = 100;

            treasureSensor = new(treasureSensorDefinition, robot.ID);
            treasureSensor.Methods["getIntensity"] = (string[] args) =>
            {
                float maxDist = 4f;
                float distance = (sensorBody.Position - new Vector3(targetBody.Position.X, sensorBody.Position.Y, targetBody.Position.Z)).Length();
                float intensity = targetIntensity * (maxDist - distance) / maxDist;

                return new string[] { intensity.ToString() };
            };

            treasureSensor.Setup(room);

            room.OnReset += (r, e) =>
            {
                chest.Position = rng.PointOnAnnulus(1.75f, 3.5f, -150f);
                chestTrigger.Position = new Vector3(chest.Position.X, 0, chest.Position.Z);
                chestTrigger.Reset();
            };
        }
    }
}