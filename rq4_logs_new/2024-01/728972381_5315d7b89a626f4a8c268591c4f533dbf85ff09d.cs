namespace VpnetworkAPI.Dto
{
    public class LocalProgramDataDto
    {
        public Guid LocalProgramDataId { get; set; }
        public string UserId { get; set; }
        public string ProgramName { get; set; }
        public double ProgramLocalNetworkThreshold { get; set; }
        public double ProgramLocalMemoryThreshold { get; set; }
        public bool? IsHarmful { get; set; }
    }
}