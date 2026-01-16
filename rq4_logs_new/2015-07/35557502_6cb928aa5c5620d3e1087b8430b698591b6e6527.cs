using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Device.Location;
using NUnit.Framework;
using Newtonsoft.Json;

namespace LSCS.Models.Tests
{
    [TestFixture]
    class ChecklistTests
    {
        [Test]
        public void TestJson()
        {
            var Checklist = new ChecklistDto {
                Title = "Test Title",
                Description = "Some random description",
                FileNumber = 12345,
                SurveyLocation = new SurveyLocation {
                    LandDistrict = new LandDistrictDto {
                        Name = "The Rocky Mountains",
                        NtsMap = "I don't know what NtsMap is",
                        Coordinate = new GeoCoordinate {
                            Latitude = 48.441271,
                            Longitude = -123.344066
                        }
                    },
                    Coordinate = new GeoCoordinate {
                        Latitude = 48.441271,
                        Longitude = -123.344066
                    },
                    Address = new CivicAddress {
                        AddressLine1 = "123 Fake Street",
                        AddressLine2 = null,
                        Building = "Buckingham Palaca",
                        City = "London",
                        CountryRegion = "England",
                        FloorLevel = null,
                        PostalCode = "123abc",
                        StateProvince = "Kenya"
                    }
                },
                Items = new List<ChecklistItem>{
                    new ChecklistItem {
                        Text = "This is the first item",
                        Status = ChecklistItemStatus.Answered
                    },
                    new ChecklistItem {
                        Text = "This is the second item",
                        Status = ChecklistItemStatus.Unanswered
                    }
                }
            };

            Console.WriteLine(JsonConvert.SerializeObject(Checklist));
        }
    }
}