/*P1*/
using System;
using System.Text.Json;
class Student
{
  private int _StudentNumber;
  public int StudentNumber   
  {
    get { return _StudentNumber; }
    set { _StudentNumber = value; }
  }
  private string _FirstName;
  public string FirstName   
  {
    get { return _FirstName; }
    set { _FirstName = value; }
  }
  private string _LastName;
  public string LastName
  {
    get { return _LastName; }
    set { _LastName = value; }
  }
}

class Scores
{
  private int _StudentNumber;
  public int StudentNumber   
  {
    get { return _StudentNumber; }
    set { _StudentNumber = value; }
  }
  private string _Lesson;
  public string Lesson   
  {
    get { return _Lesson; }
    set { _Lesson = value; }
  }
  private int _Score;
  public int Score
  {
    get { return _Score; }
    set { _Score = value; }
  }   
}


class Program 
{
    public static void Main(String[] args)
    {
        JsonSerializerOptions _options = new()
        {
            PropertyNameCaseInsensitive = true
        };
        var json_students = File.ReadAllText("./files/students.json");
        List<Student> students = JsonSerializer.Deserialize<List<Student>>(json_students, _options);

        var json_scores = File.ReadAllText("./files/scores.json");
        List<Student> scores = JsonSerializer.Deserialize<List<Student>>(json_scores, _options);

    }
}