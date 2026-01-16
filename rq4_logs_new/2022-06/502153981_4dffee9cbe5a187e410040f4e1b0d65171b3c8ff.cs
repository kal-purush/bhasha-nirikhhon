using PikTestPlugin.Models;

namespace PikTestPlugin
{
    internal interface IPainter
    {
        string RoomCalculatedSubzoneIdParameterName { get; }
        string RoomSubzoneIndexParameterName { get; }
        string RoomSufixToPaint { get; }

        void PaintAdjacent(ApartmentComplex complex);
        void PaintAdjacent(Section section);
        void PaintAdjacent(Level level);
        void PaintAdjacent(ApartmentLayout apartmentLayout);
        void PaintAdjacent(Apartment apartment);
    }
}