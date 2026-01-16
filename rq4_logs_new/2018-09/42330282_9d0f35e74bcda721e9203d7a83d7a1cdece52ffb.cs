using System.Collections.Generic;

namespace LG.Eloqua.Api.Rest.ClientLibrary.Models.Data.Assets.Campaign
{
    [Resource("/assets/campaign", "Campaign", "/api/REST/2.0")]
    public class Campaign : IEloquaDataObject
    {
        public int? Id { get; set; }

        public int? CreatedAt { get; set; }
        public int? CreatedBy { get; set; }
        public int? FolderId { get; set; }
        public string Name { get; set; }
        public int? UpdatedAt { get; set; }
        public int? UpdatedBy { get; set; }
        public float? ActualCost { get; set; }
        public float? BudgetedCost { get; set; }
        public string CampaignType { get; set; }
        public List<CampaignRelatedElement> Elements { get; set; }
        public int? StartAt { get; set; }
        public int? EndAt { get; set; }
        public bool? IsIncludedInRoi { get; set; }
        public bool? IsMemberAllowedReEntry { get; set; }
        public bool? IsReadOnly { get; set; }
        public bool? IsSyncedWithCrm { get; set; }
        public int? RunAsUserId { get; set; }
    }
}