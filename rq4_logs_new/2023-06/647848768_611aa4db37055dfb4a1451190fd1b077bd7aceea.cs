using Newtonsoft.Json;

class User // класс Юзер
{
    public string Name { get; private set; } // свойства класса 
    public string Surname { get; private set; }
    public int Id { get; private set; }
    public int CountRightAnswers { get; private set; }
    public string Diagnose { get; private set; }


    public void SetNewId(int id)
    {
        Id = id;
    }


    public override string ToString()
    {
        return $" {Id}  {Name} {Surname} {CountRightAnswers} {Diagnose}";
    }


    public User(int id, string name, string surname, int countRightAnswers, string diagnose) // конструктор который принимает свойства
    {
        Id = id;
        Name = name;
        Surname = surname;
        CountRightAnswers = countRightAnswers;
        Diagnose = diagnose;

    }

    
    

    

}


