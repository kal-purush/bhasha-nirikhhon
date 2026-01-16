using UProgress.Contracts.Core;

namespace UProgress.Contracts.Models;

#region Models

public abstract class BaseEntity<TId>
{
    public TId Id { get; set; }
}

public abstract class BaseEntity : BaseEntity<Guid>
{
}

public class User : BaseEntity
{
    public string FullName { get; set; }
    public UserType Role { get; set; }
    public Guid? GroupId { get; set; }
    public SubGroupType? SubGroup { get; set; }

    public virtual Group Group { get; set; }
    public virtual Group HeadGroup { get; set; }
    public virtual ICollection<StudentDiscipline> StudentDisciplines { get; set; }
    public virtual ICollection<TaskAnswer> StudentTaskAnswers { get; set; }
    public virtual ICollection<TaskAnswer> TeacherApprovedTaskAnswers { get; set; }
    public virtual ICollection<TeacherGroupDiscipline> TeacherGroupDisciplines { get; set; }
    public virtual ICollection<AnswerHistory> UserTaskAnswerHistory { get; set; }
}

public class Speciality : BaseEntity
{
    public string ShortName { get; set; }
    public string Name { get; set; }
    public int SemesterCount { get; set; }

    public virtual ICollection<Discipline> Disciplines { get; set; }
    public virtual ICollection<Group> Groups { get; set; }
}

public class Group : BaseEntity
{
    public int StartYear { get; set; }
    public int GraduatedYear { get; set; }
    public Guid HeadId { get; set; }
    public Guid SpecialityId { get; set; }

    public virtual User Head { get; set; }
    public virtual Speciality Speciality { get; set; }
    public virtual ICollection<User> Students { get; set; }
    public virtual ICollection<TeacherGroupDiscipline> TeacherGroupDisciplines { get; set; }
}

public class TeacherGroupDiscipline : BaseEntity
{
    public SubGroupType SubGroup { get; set; }
    public Guid TeacherId { get; set; }
    public Guid GroupId { get; set; }
    public Guid DisciplineId { get; set; }

    public virtual User Teacher { get; set; }
    public virtual Group Group { get; set; }
    public virtual Discipline Discipline { get; set; }
}

public class StudentDiscipline : BaseEntity
{
    public bool IsAvailable { get; set; }
    public int? Mark { get; set; }
    public Guid StudentId { get; set; }
    public Guid DisciplineId { get; set; }

    public virtual User Student { get; set; }
    public virtual Discipline Discipline { get; set; }
}

public class Discipline : BaseEntity
{
    public string Name { get; set; }
    public string Description { get; set; }
    public int? Semester { get; set; }
    public DisciplineType Type { get; set; }
    public Guid? SpecialityId { get; set; }

    public virtual Speciality Speciality { get; set; }
    public virtual ICollection<Task> Tasks { get; set; }
    public virtual ICollection<StudentDiscipline> DisciplineStudents { get; set; }
    public virtual ICollection<TeacherGroupDiscipline> TeacherGroupDisciplines { get; set; }
}

public class Task : BaseEntity
{
    public string Name { get; set; }
    public string Description { get; set; }
    public bool IsRequired { get; set; }
    public Guid DisciplineId { get; set; }

    public virtual Discipline Discipline { get; set; }
    public virtual ICollection<TaskAttachment> Attachments { get; set; }
    public virtual ICollection<TaskAnswer> Answers { get; set; }
}

public class TaskAttachment : BaseEntity
{
    public string Name { get; set; }
    public string Extension { get; set; }
    public Guid TaskId { get; set; }

    public virtual Task Task { get; set; }
}

public class TaskAnswer : BaseEntity
{
    public string Answer { get; set; }
    public AnswerStatus Status { get; set; }
    public Guid TaskId { get; set; }
    public Guid StudentId { get; set; }
    public Guid? ApprovedById { get; set; }

    public virtual Task Task { get; set; }
    public virtual User Student { get; set; }
    public virtual User ApprovedBy { get; set; }
    public virtual ICollection<AnswerAttachment> Attachments { get; set; }
    public virtual ICollection<AnswerHistory> History { get; set; }
}

public class AnswerHistory : BaseEntity
{
    public DateTime Date { get; set; }
    public AnswerStatus StatusMovedTo { get; set; }
    public string? Message { get; set; }
    public Guid UserId { get; set; }
    public Guid AnswerId { get; set; }

    public virtual User User { get; set; }
    public virtual TaskAnswer Answer { get; set; }
}

public class AnswerAttachment : BaseEntity
{
    public string Name { get; set; }
    public string Extension { get; set; }
    public Guid AnswerId { get; set; }

    public virtual TaskAnswer Answer { get; set; }
}

#endregion

#region Enums

public enum UserType
{
    Dean,
    Teacher,
    Student
}

public enum DisciplineType
{
    Exam,
    Project,
    Mark,
    NoMark,
    Free
}

public enum AnswerStatus
{
    Draft,
    Approvable,
    Rejected,
    Approved
}

public enum SubGroupType
{
    Full,
    First,
    Second
}

#endregion