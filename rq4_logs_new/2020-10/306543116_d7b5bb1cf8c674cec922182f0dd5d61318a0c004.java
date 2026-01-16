import java.util.Comparator;

public class CatalogComparator implements Comparator<CatalogModel> {
    @Override
    public int compare(CatalogModel o1, CatalogModel o2) {
        return o1.getName().compareTo(o2.getName());
    }
}