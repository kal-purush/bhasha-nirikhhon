import java.util.Comparator;

public class SortByDeadline implements Comparator<Task> {

    @Override
    public int compare(Task d1, Task d2) {
        if (!(d1 instanceof Deadline) && !(d2 instanceof Deadline)) {
            return 0;
        } else if (!(d1 instanceof Deadline)) {
            return 1;
        } else if (!(d2 instanceof Deadline)) {
            return -1;
        }
        return ((Deadline) d1).getDeadlineTime().compareTo(((Deadline) d2).getDeadlineTime());
    }
}