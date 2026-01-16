

namespace UniversityEmployee;

internal class DegreeTeacher:Teacher
{
    public string ScienceDegree { get; set; }
    public string Rank { get; set; }

    public DegreeTeacher ( string person, string taxID, Course course, string scienceDegree, string rank) :
                    base (person, taxID, course)
    {
        ScienceDegree = scienceDegree;
        Rank = rank;
    }
    public override string GetOfficialDuties()
    {
        return base.GetOfficialDuties() + ScienceDegree + " " + Rank;
    }
}