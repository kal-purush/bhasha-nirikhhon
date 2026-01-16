import java.time.LocalDate;
import java.util.ArrayList;

public class CourseProgramme
{
    private String name;
    private ArrayList<Module> modules = new ArrayList<Module>();
    private ArrayList<Student> students = new ArrayList<Student>();
    private LocalDate startDate;
    private LocalDate endDate;

    public CourseProgramme(String courseName, LocalDate startDate, LocalDate endDate)
    {
        name = courseName;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public LocalDate getStartDate()
    {
        return startDate;
    }

    public void setStartDate(LocalDate start)
    {
        startDate = start;
    }

    public LocalDate getEndDate()
    {
        return endDate;
    }

    public void setEndDate(LocalDate end)
    {
        endDate = end;
    }

    public void addStudent(Student student)
    {
        students.add(student);
    }

    public void removeStudent(Student student)
    {
        students.remove(student);
    }

    public ArrayList getStudents()
    {
        return students;
    }

    public void setStudents(ArrayList<Student> students)
    {
        this.students = students;
    }

    public void addModule(Module module)
    {
        modules.add(module);
    }

    public void removeModule(Module module)
    {
        modules.remove(module);
    }

    public ArrayList getModule()
    {
        return modules;
    }

    public void setModules(ArrayList<Module> modules)
    {
        this.modules = modules;
    }
}