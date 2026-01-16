using UnityEngine;

namespace Terrain
{
    public class Terrain : MonoBehaviour
    {
        public int DefensePercentage = 0;
        public int MovementCost = 1;
        public string DisplayName;
        public int HitPercentage = 0;

        public virtual bool IsPassable(Character character, float x, float y)
        {
            return true;
        }

        public bool IsPassable(Character character, Vector2 position)
        {
            return IsPassable(character, position.x, position.y);
        }
    }
}