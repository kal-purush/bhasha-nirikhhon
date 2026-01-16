using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BuckingshireImporter.Services;

internal class BuckinghapshireService
{
    public int number { get; set; } = default!;
    public int size { get; set; } = default!;
    public int totalPages { get; set; } = default!;
    public int totalElements { get; set; } = default!;
    public bool first { get; set; } = default!;
    public bool last { get; set; } = default!;
    public Content[] content { get; set; } = default!;
}

internal class Content
{
    public int id { get; set; } = default!;
    public DateTime updated_at { get; set; } = default!;
    public string name { get; set; } = default!;
    public string description { get; set; } = default!;
    public string url { get; set; } = default!;
    public int? min_age { get; set; } = default!;
    public int? max_age { get; set; } = default!;
    public object age_band_under_2 { get; set; } = default!;
    public object age_band_2 { get; set; } = default!;
    public object age_band_3_4 { get; set; } = default!;
    public object age_band_5_7 { get; set; } = default!;
    public object age_band_8_plus { get; set; } = default!;
    public object age_band_all { get; set; } = default!;
    public object needs_referral { get; set; } = default!;
    public bool? free { get; set; } = default!;
    public DateTime created_at { get; set; } = default!;
    public string status { get; set; } = default!;
    public TargetDirectories[] target_directories { get; set; } = default!;
    public Location[] locations { get; set; } = default!;
    public Contact[] contacts { get; set; } = default!;
    public Meta[] meta { get; set; } = default!;
    public Organisation organisation { get; set; } = default!;
    public Taxonomy[] taxonomies { get; set; } = default!;
    public RegularSchedules[] regular_schedules { get; set; } = default!;
    public CostOptions[] cost_options { get; set; } = default!;
    public Link[] links { get; set; } = default!;
    public object[] send_needs { get; set; } = default!;
    public Suitability[] suitabilities { get; set; } = default!;
    public LocalOffer local_offer { get; set; } = default!;
    public object distance_away { get; set; } = default!;
}

internal class Organisation
{
    public int id { get; set; } = default!;
    public string name { get; set; } = default!;
    public string description { get; set; } = default!;
    public string email { get; set; } = default!;
    public string url { get; set; } = default!;
}

internal class LocalOffer
{
    public string description { get; set; } = default!;
    public string link { get; set; } = default!;
    public SurveyAnswers[] survey_answers { get; set; } = default!;
}

internal class SurveyAnswers
{
    public string question { get; set; } = default!;
    public string answer { get; set; } = default!;
}   

internal class TargetDirectories
{
    public string name { get; set; } = default!;
    public string label { get; set; } = default!;
}

internal class Location
{
    public int id { get; set; } = default!;
    public string name { get; set; } = default!;
    public string address_1 { get; set; } = default!;
    public string city { get; set; } = default!;
    public string state_province { get; set; } = default!;
    public string postal_code { get; set; } = default!;
    public string country { get; set; } = default!;
    public Geometry geometry { get; set; } = default!;
    public bool? mask_exact_address { get; set; } = default!;
    public Accessibility[] accessibilities { get; set; } = default!;
}

internal class Geometry
{
    public string type { get; set; } = default!;
    public float[] coordinates { get; set; } = default!;
}

internal class Accessibility
{
    public string name { get; set; } = default!;
    public string slug { get; set; } = default!;
}

internal class Contact
{
    public int id { get; set; } = default!;
    public string name { get; set; } = default!;
    public string title { get; set; } = default!;
    public string email { get; set; } = default!;
    public string phone { get; set; } = default!;
}

internal class Meta
{
    public string key { get; set; } = default!;
    public string value { get; set; } = default!;
}

internal class Taxonomy
{
    public int id { get; set; } = default!;
    public string name { get; set; } = default!;
    public string slug { get; set; } = default!;
    public int? parent_id { get; set; } = default!;
}

internal class RegularSchedules
{
    public int id { get; set; } = default!;
    public string weekday { get; set; } = default!;
    public string opens_at { get; set; } = default!;
    public string closes_at { get; set; } = default!;
}

internal class CostOptions
{
    public int id { get; set; } = default!;
    public string option { get; set; } = default!;
    public string amount { get; set; } = default!;
    public string cost_type { get; set; } = default!;
}

internal class Link
{
    public string label { get; set; } = default!;
    public string url { get; set; } = default!;
}

internal class Suitability
{
    public int id { get; set; } = default!;
    public string name { get; set; } = default!;
    public string slug { get; set; } = default!;
}