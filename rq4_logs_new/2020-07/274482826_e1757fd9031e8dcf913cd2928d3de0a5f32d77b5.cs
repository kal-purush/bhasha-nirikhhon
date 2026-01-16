using System.Collections.Generic;

namespace PixelHunter1995
{
    using Dog = System.ValueTuple<float, float, float, float>;

    //using Background = System.ValueTuple<>;
    class Scene
    {
        public string background;
        public List<Dog> dogs;
        public WalkingArea walkingArea;

        public Scene(string background, List<Dog> dogs, WalkingArea walkingArea)
        {
            this.background = background;
            this.dogs = dogs;
            this.walkingArea = walkingArea;
        }
    }
}