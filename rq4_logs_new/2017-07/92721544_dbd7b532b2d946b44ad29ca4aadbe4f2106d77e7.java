package building;

import java.util.ArrayList;
import java.util.List;

public class StairwayGraphPoint extends GraphPoint
{
    private List<GraphPoint> stairwaySiblings = new ArrayList<>();
    
    public StairwayGraphPoint(Double x, Double y, String name)
    {
        super(x, y, name);
    }
    
    public StairwayGraphPoint(List<Double> points, String name)
    {
        this(points.get(0), points.get(1), name);
    }
    
    public List<GraphPoint> getStairwaySiblings(){ return stairwaySiblings;}

    public void addSibling(StairwayGraphPoint sibling){stairwaySiblings.add(sibling);}
    
    @Override
    public Double calculateDistanceToNeighbour(GraphPoint point)
    {
        if(point instanceof StairwayGraphPoint)
        {
            return 0.0;
        }

        return super.calculateDistanceToNeighbour(point);
    }
}