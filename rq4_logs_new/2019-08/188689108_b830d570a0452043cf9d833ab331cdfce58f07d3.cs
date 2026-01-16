using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using vopen_api.Data;

namespace vopen_api.Models.legacy
{
    public class LegacyConfSponsorsDTO
    {
        public readonly string ImageBaseURL = "";
        public readonly string Info = "All images are .png";
        public ICollection<LegacyConfSponsorDTO> ConfSponsors { get; set; }
    }

    public class LegacyConfSponsorDTO
    {
        [JsonProperty("SponsorId")]
        public string SponsorId { get; set; }

        [JsonProperty("SponsorCategoryId")]
        public int SponsorCategoryId { get; set; }

        [JsonProperty("HasBooth")]
        public bool HasBooth { get; set; }

        [JsonProperty("Sponsor")]
        public LegacySponsorDTO Sponsor { get; set; }

        [JsonProperty("SponsorCategory")]
        public SponsorCategory SponsorCategory { get; set; }
    }

    public class SponsorCategory
    {
        [JsonProperty("SponsorCategoryId")]
        public int SponsorCategoryId { get; set; }

        [JsonProperty("Name")]
        public string Name { get; set; }

        [JsonProperty("HasBooth")]
        public bool HasBooth { get; set; }
    }

    public static class LegacyConfSponsorsUtils
    {
        public static LegacyConfSponsorsDTO ToLegacyConfSponsorsDTO(ICollection<EditionSponsorDTO> sponsors)
        {
            var legacyConfSponsorsDTO = new LegacyConfSponsorsDTO
            {
                ConfSponsors = sponsors.Select(
                    sponsor => new LegacyConfSponsorDTO
                    {
                        SponsorId = sponsor.Id,
                        Sponsor = new LegacySponsorDTO(sponsor),
                        SponsorCategory = LegacyConfSponsorsUtils.GetSponsorCategory(sponsor.Type),
                        SponsorCategoryId = LegacyConfSponsorsUtils.GetSponsorCategory(sponsor.Type).SponsorCategoryId,
                        HasBooth = LegacyConfSponsorsUtils.GetSponsorCategory(sponsor.Type).HasBooth,
                    }
                ).ToList()
            };

            return legacyConfSponsorsDTO;
        }

        public static SponsorCategory GetSponsorCategory(string type)
        {
            var categoryId = type == Constants.SPONSOR_DIAMOND? 6
                : type == Constants.SPONSOR_GOLD ? 4
                    : type == Constants.SPONSOR_SILVER ? 2
                        : 1;

            var hasBooth = type != Constants.SPONSOR_DIGITAL;

            return new SponsorCategory
            {
                SponsorCategoryId = categoryId,
                Name = type,
                HasBooth = hasBooth
            };
        }
    }
}