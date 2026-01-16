import pytest

from listings.models import HotelRoom, HotelRoomType, Listing, BookingInfo, Reservation


@pytest.mark.django_db
def test_HotelRoom():
    assert HotelRoom.objects.count() == 0


@pytest.mark.django_db
def test_str_HotelRoom():
    sub_title = HotelRoom.objects.create(room_number="2")
    assert str(sub_title.room_number) == "2"


@pytest.mark.django_db
def test_HotelRoomType():
    assert HotelRoomType.objects.count() == 0


@pytest.mark.django_db
def test_str_HotelRoomType():
    sub_title = Listing.objects.create(
        listing_type="hotel", title="Test", country="Test", city="Test"
    )
    sub_title = HotelRoomType.objects.create(title="Test", hotel=sub_title)
    assert str(sub_title.title) + " - " + str(sub_title) == "Test - Test - Test"


@pytest.mark.django_db
def test_Listing():
    assert Listing.objects.count() == 0


@pytest.mark.django_db
def test_str_Listing():
    sub_title = Listing.objects.create(
        listing_type="hotel", title="Test", country="Test", city="Test"
    )
    assert str(sub_title.title) == "Test"


@pytest.mark.django_db
def test_BookingInfo():

    assert BookingInfo.objects.count() == 0


@pytest.mark.django_db
def test_str_BookingInfo():
    sub_listing = Listing.objects.create(
        listing_type="hotel", title="Test", country="Test", city="Test"
    )
    sub_hotel_room_type = HotelRoomType.objects.create(title="Test", hotel=sub_listing)

    sub_title = BookingInfo.objects.create(
        listing=sub_listing,
        hotel_room_type=sub_hotel_room_type,
        price=2.00,
    )
    assert str(f"{sub_title.listing} {sub_title.price}") == "Test 2.0"


@pytest.mark.django_db
def test_Reservation():
    assert Reservation.objects.count() == 0


@pytest.mark.django_db
def test_str_Reservation():
    hotel_room = HotelRoom.objects.create(room_number="2")
    sub_hotel_room = Reservation.objects.create(hotel_room=hotel_room)
    assert str(sub_hotel_room) == "2"