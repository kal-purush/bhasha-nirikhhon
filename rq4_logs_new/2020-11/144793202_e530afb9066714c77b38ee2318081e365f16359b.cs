using MazeWalker.Domain.Location;

namespace MazeWalker.Domain.Walker
{
    public interface IHand
    {
        Direction GetCounterDirection(Direction faceDirection);
        Direction GetDirection(Direction faceDirection);
    }
}