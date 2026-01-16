using System.Collections.Generic;

namespace Martium.TravelInfo.CustomMapsApiClient.Contracts.Internal
{
    internal class BingMapsApiRouteResponse
    {
        public int StatusCode { get; set; }
        public string StatusDescription { get; set; }
        public string TraceId { get; set; }
        public List<ResourceSet> ResourceSets { get; set; }
        public List<string> ErrorDetails { get; set; }
    }
    internal class ResourceSet
    {
        public int EstimatedTotal { get; set; }
        public List<Resource> Resources { get; set; }
    }
    internal class Resource
    {
        public string DistanceUnit { get; set; }
        public string DurationUnit { get; set; }
        public List<RouteLeg> RouteLegs { get; set; }
        public RoutePath RoutePath { get; set; }
        public string TrafficCongestion { get; set; }
        public string TrafficDataUsed { get; set; }
        public double TravelDistance { get; set; }
        public int TravelDuration { get; set; }
        public int TravelDurationTraffic { get; set; }
        public string TravelMode { get; set; }
    }
    internal class RouteLeg
    {
        public RouteLegStartLocation StartLocation { get; set; }
        public RouteLegStartLocation EndLocation { get; set; }
    }
    internal class RoutePath
    {
        public Line Line { get; set; }
    }
    internal class Line
    {
        public List<List<double>> Coordinates { get; set; }
    }
    internal class RouteLegStartLocation
    {
        public string Name { get; set; }
        public Point Point { get; set; }
        public Address Address { get; set; }
        public string Confidence { get; set; }
        public string EntityType { get; set; }
    }
    internal class Point
    {
        public string Type { get; set; }
        public List<List<double>> Coordinates { get; set; }
    }
    internal class Address
    {
        public string AddressLine { get; set; }
        public string AdminDistrict { get; set; }
        public string CountryRegion { get; set; }
        public string FormattedAddress { get; set; }
        public string Locality { get; set; }
        public string PostalCode { get; set; }
    }
}