namespace PikTestPlugin.Models
{
    public static class ParametersNames
    {
        private const string _roomNumberOfRoomsParameterName = "ROM_Подзона";
        private const string _apartmentParameterName = "ROM_Зона";
        private const string _apartmentPurposeParameterName = "Квартира";
        private const string _sectionParameterName = "BS_Блок";

        public static string RoomNumberOfRoomsParameterName => _roomNumberOfRoomsParameterName;
        public static string ApartmentParameterName => _apartmentParameterName;
        public static string ApartmentPurposeParameterName => _apartmentPurposeParameterName;
        public static string SectionParameterName => _sectionParameterName;
    }
}