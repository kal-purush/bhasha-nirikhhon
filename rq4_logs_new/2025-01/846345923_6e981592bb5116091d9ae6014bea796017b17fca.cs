namespace FConfig.Src;

public sealed class SnowflakeIdOption
{
    public uint MachineId { get; init; }

    public CustomEpochOption CustomEpoch { get; init; } = new();

    public int MachineIDBitLength { get; init; }

    public int SequenceBitLength { get; init; }

    public sealed class CustomEpochOption
    {
        public int Year { get; init; }

        public int Month { get; init; }

        public int Day { get; init; }

        public int Hour { get; init; }

        public int Minute { get; init; }

        public int Second { get; init; }
    }
}