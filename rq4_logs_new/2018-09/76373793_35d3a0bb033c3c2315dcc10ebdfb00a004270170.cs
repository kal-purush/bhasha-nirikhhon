using System;
using System.Collections.Generic;
using System.Text;

namespace Xena.Contracts.Domain
{
    public class ActivityLogDto : FiscalDto, IHasIdDto
    {
        public long? Id { get; set; }
        public int DateDays { get; set; }
        public string Description { get; set; }
        public long ResourceId { get; set; }
        public long ChangerId { get; set; }
        public string ResourceName { get; set; }
        public string ChangerName { get; set; }
    }
}