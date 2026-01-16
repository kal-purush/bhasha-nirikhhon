/**
 *
 * @author joey
 */
public class GpaCompAscending implements Comparator<Student> {
    GpaComp gpac = new GpaComp();
    
    /**
     * @param t1 Student 1
     * @param t2 Student 2
     * @return comparison
     */
    @Override
    public int compare(Student t1, Student t2) {
        return gpac.compare(t1, t2);
    }
    
}