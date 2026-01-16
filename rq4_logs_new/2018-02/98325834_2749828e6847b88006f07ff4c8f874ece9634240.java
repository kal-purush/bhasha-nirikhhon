public class QTreeNode {
    String index;
    double ul_lon;
    double ul_lat;
    double lr_lon;
    double lr_lat;

    public QTreeNode(String index,
                     double ul_lon,
                     double ul_lat,
                     double lr_lon,
                     double lr_lat){
        this.index = index;
        this.ul_lon = ul_lon;
        this.ul_lat = ul_lat;
        this.lr_lon = lr_lon;
        this.lr_lat = lr_lat;
    }

    public boolean intersectsTile(double query_ul, double query_lr){
        return false;
    }

    public boolean lonDPPsmallerThanOrIsLeaf(double queriesLonDPP){
        return false;
    }
}