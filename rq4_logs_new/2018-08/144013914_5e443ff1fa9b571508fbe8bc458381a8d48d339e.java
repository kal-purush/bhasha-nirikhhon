public class DepthFilter implements Filter{
    private double mindepth;
    private double maxdepth;
    //constructor
    public DepthFilter(double min, double max){
        mindepth = min;
        maxdepth = max;
    }

    public boolean satisfies(QuakeEntry qe) {
        if (qe.getDepth()>=mindepth && qe.getDepth()<=maxdepth){
            return true;
        }
        else{
            return false;
        }
    }
}