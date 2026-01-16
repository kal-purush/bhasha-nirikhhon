using GoogleMaps_FinalProjectAspApi.SearchRequestCreation;
using System.Text.Json.Serialization;
using System.Text.Json.Nodes;

namespace GoogleMaps_FinalProjectAspApi.Models
{
	public class PlaceDetails
	{
		[JsonPropertyName("displayName")]
		public string Name { get; set; }

		[JsonPropertyName("rating")]
		public double Rating { get; set; }

		[JsonPropertyName("userRatingsCount")]
		public int RatingsCount { get; set; }

		[JsonPropertyName("formattedAddress")]
		public string Address { get; set; }

		[JsonPropertyName("weekdayDescriptions")]
		public JsonArray WeekdayDescriptions { get; set; }

		[JsonPropertyName("photos")]
		public JsonArray Photos { get; set; }
	}
}