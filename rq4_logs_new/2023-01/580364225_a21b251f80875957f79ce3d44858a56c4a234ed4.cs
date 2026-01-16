namespace UniversityEmployee;

public class ComparerLengthOfName : IComparer<UniversityEmployee>
{
    int IComparer<UniversityEmployee>.Compare(UniversityEmployee? x, UniversityEmployee? y)
    {
        if (x.Person.FullNameLength() > y.Person.FullNameLength())
        {
            return -1;
        }
        else if (x.Person.FullNameLength() < y.Person.FullNameLength())
        {
            return 1;
        }
        else
            return 0;
    }
}