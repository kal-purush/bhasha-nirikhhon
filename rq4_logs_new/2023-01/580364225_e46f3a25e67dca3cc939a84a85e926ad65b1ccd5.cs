

namespace UniversityEmployee;

internal class Address
{
    private const int max_builging_number = 1000;
    private const int max_room_number = 10000;
    private int _numberOfBuilding;
    private int _numberOfRoom;
    public string City { get; set; }
    public string Street { get; set; }
    public int NumberOfBuilding
    {
        get
        {
            return _numberOfBuilding;
        }
        set
        { 
            if (value > 0 && value < max_builging_number)
            {
                _numberOfBuilding = value;
            }
        }
    }
    public int NumberOfRoom
    {
        get
        {
            return _numberOfRoom;
        }
        set
        {
            if (value > 0 && value < max_room_number)
            {
                _numberOfRoom = value;
            }
        }
    }

    public Address(string city, string street, int numberOfHouse, int numberOfFlat)
    {
        City = city;
        Street = street;
        NumberOfBuilding = numberOfHouse;
        NumberOfRoom = numberOfFlat;
    }
}