using System;

namespace SS.Api.models.dto.generated
{
    public partial class SheriffActingRankDto
    {
        public string Rank { get; set; }
        public int Id { get; set; }
        public DateTimeOffset StartDate { get; set; }
        public DateTimeOffset EndDate { get; set; }
        public DateTimeOffset? ExpiryDate { get; set; }
        public string ExpiryReason { get; set; }
        public Guid SheriffId { get; set; }
        public string Comment { get; set; }
        public string Timezone { get; set; }
        public uint ConcurrencyToken { get; set; }
    }
}