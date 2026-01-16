using System.ComponentModel.DataAnnotations;

namespace LogicLayer.Models;

public class LessonProgress
{
    public LessonProgress(int accountId, int lessonId, int progress)
    {
        AccountId = accountId;
        LessonId = lessonId;
        Progress = progress;
    }

    public LessonProgress(int accountId = 0, int unitId = 0)
    {
        AccountId = accountId;
        LessonId = unitId;
    }

    public LessonProgress()
    {
    }
    
    [Key]
    public int AccountId { get; set; }
    
    [Key]
    public int LessonId { get; set; }
    
    public int Progress { get; set; }
}