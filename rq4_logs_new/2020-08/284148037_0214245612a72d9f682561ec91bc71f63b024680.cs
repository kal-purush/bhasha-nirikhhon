using System;

namespace TransmitterCalculator.Settings
{
    public sealed class CoordinateSystemSettings
    {
        private static readonly Lazy<CoordinateSystemSettings> _coordinateSystemSettings = new Lazy<CoordinateSystemSettings>(() => new CoordinateSystemSettings());

        public static CoordinateSystemSettings Instance => _coordinateSystemSettings.Value;

        public int BoardWidth { get; private set; }
        public int BoardHeight { get; private set; }
        public int XZeroPointCooridnate { get; private set; }
        public int YZeroPointCoordinate { get; private set; }
        public int MinXCoordinate { get; private set; }
        public int MinYCoordinate { get; private set; }
        public int MaxXCoordinate { get; private set; }
        public int MaxYCoordiante { get; private set; }

        public void Initialize(int windowWidth, int windowHeight)
        {
            BoardWidth = windowWidth;
            BoardHeight = windowHeight;
            XZeroPointCooridnate = windowWidth / 2;
            YZeroPointCoordinate = windowHeight / 2;
            MinXCoordinate = (XZeroPointCooridnate / UnitSize.UNIT_WIDTH) * -1;
            MinYCoordinate = (YZeroPointCoordinate / UnitSize.UNIT_WIDTH) * -1;
            MaxXCoordinate = MinXCoordinate * -1;
            MaxYCoordiante = MinYCoordinate * -1;
        }


        private CoordinateSystemSettings()
        {
        }
    }
}