package api;

/**
 * Created by Vincent on 21/2/2017.
 */
public interface IAdmin {

    /**
     * Create class {@code className} with capacity {@code capacity} and assign instructor {@code instructorName}
     * The className/year pair must be unique and no instructor can be assigned to more than two courses in a year.
     *
     * @param className Name of the class to be created
     * @param year Calendar year in which the course is to be taught, cannot be in the past
     * @param instructorName Name of instructor to be assigned,
     * @param capacity Maximum capacity of this class > 0
     */
    public void createClass(String className, int year, String instructorName, int capacity);

    /**
     * Adjust the capacity (maximum number of students) of class {@code className} to new capacity {@code capacity}
     *
     * @param className Name of the class for which capacity should be changed
     * @param year Year in which this class is taught
     * @param capacity New capacity of this class, must be at least equal to the number of students enrolled
     */
    public void changeCapacity(String className, int year, int capacity);

    /**
     * @return Whether class {@code className} exists in year {@code year}
     */
    public boolean classExists(String className, int year);

    /**
     * @return The name of the instructor for class {@code className} in year {@code year}
     */
    public String getClassInstructor(String className, int year);

    /**
     * @return The capacity (maximum number of enrollees) for class {@code className} in year {@code year}
     */
    public int getClassCapacity(String className, int year);
}