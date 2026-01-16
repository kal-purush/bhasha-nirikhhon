using System.Collections.Generic;
using Machine.Specifications;
using Rhino.Mocks;
using Rhino.Mspec.Contrib;
using zavit.Domain.Activities;
using zavit.Domain.Venues.DefaultVenues;
using zavit.Domain.Venues.PublicPlaces;

namespace zavit.Domain.Venues.Tests.DefaultVenues 
{
    [Subject("DefaultVenueProvider")]
    public class DefaultVenueProviderTests : TestOf<DefaultVenueProvider>
    {
        class When_providing_a_default_venue_for_a_public_place
        {
            Because of = () => _result = Subject.ProvideDefaultVenue(_publicPlace);

            It should_set_the_venue_name_to_be_the_same_as_the_name_of_the_place =
                () => _result.Name.ShouldEqual(_publicPlace.Name);

            It should_set_the_venue_activities_to_be_default_activities =
                () => _result.Activities.ShouldEqual(_activities);

            It should_set_the_venue_address_to_be_the_public_place_address =
                () => _result.Address.ShouldEqual(_publicPlace.Address);

            It should_set_public_place_id_to_be_the_same_as_the_public_place =
                () => _result.PublicPlaceId.ShouldEqual(_publicPlace.PlaceId);

            It should_set_latitude_to_be_the_same_as_the_public_place =
                () => _result.Latitude.ShouldEqual(_publicPlace.Latitude);

            It should_set_longitude_to_be_the_same_as_the_public_place =
                () => _result.Longitude.ShouldEqual(_publicPlace.Longitude);

            Establish context = () =>
            {
                _publicPlace = NewInstanceOf<PublicPlace>();
                _publicPlace.Name = "Place name";
                _publicPlace.Address = "Test address";
                _publicPlace.PlaceId = "PublicPlaceID";
                _publicPlace.Latitude = 1;
                _publicPlace.Longitude = -0.5;

                _activities = new[] { NewInstanceOf<Activity>() };
                Injected<IActivityRepository>().Stub(r => r.GetDefaultActivities()).Return(_activities);
            };

            static IList<Activity> _activities;
            static PublicPlace _publicPlace;
            static Venue _result;
        }
    }
}
