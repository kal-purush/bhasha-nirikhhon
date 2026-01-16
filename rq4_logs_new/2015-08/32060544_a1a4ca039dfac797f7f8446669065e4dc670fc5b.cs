
namespace TerrLuo.DesignPattern.Prototype.FactoryMethod
{
    public class DownArrowTool : GeometryTool
    {
        public override Geometry Create()
        {
            return new DownArrow();
        }
    }
}