using Newtonsoft.Json;

internal class FileSystem
{
    public string FileDBName;
    public string FileFolderPath;
    public string DBFilePath;

    public void ClearDB()
    {

        File.WriteAllText(DBFilePath, "");
        Console.WriteLine("Готово");

    }


    public void SaveUsersToDB(List<User> users)
    {

        string serializedUsers = JsonConvert.SerializeObject(users);
        File.WriteAllText(DBFilePath, serializedUsers);

    }

    public List<User> ReadAllUsersFromDB()
    {

        string json = File.ReadAllText(DBFilePath);
        List<User> currentUsers = JsonConvert.DeserializeObject<List<User>>(json);


        return currentUsers ?? new List<User>();

    }


    public void SaveUsersToDB(User user)
    {
        List<User> AllCurrentUsers = ReadAllUsersFromDB();
        int lastId = AllCurrentUsers.Count == 0 ? 0 : AllCurrentUsers.Last().Id;

        user.SetNewId(lastId + 1);

        AllCurrentUsers.Add(user);
        string serializedUsers = JsonConvert.SerializeObject(AllCurrentUsers);
        File.WriteAllText(DBFilePath, serializedUsers);


    }


    public void SaveQuestionsToDB(List<Question> questions)
    {

        string serializedQuestions = JsonConvert.SerializeObject(questions);
        File.WriteAllText(DBFilePath, serializedQuestions);

    }

    public List<Question> ReadAllQuestionsFromDB()
    {

        string json = File.ReadAllText(DBFilePath);
        List<Question> currentQuestions = JsonConvert.DeserializeObject<List<Question>>(json);


        return currentQuestions ?? new List<Question>();

    }


    public void SaveToDB(Question question)
    {
        List<Question> AllCurrentQuestions = ReadAllQuestionsFromDB();
        int lastNumber = AllCurrentQuestions.Count == 0 ? 0 : AllCurrentQuestions.Last().Number;

        question.SetNewNumber(lastNumber + 1);

        AllCurrentQuestions.Add(question);
        string serializedQuestion = JsonConvert.SerializeObject(AllCurrentQuestions);
        File.WriteAllText(DBFilePath, serializedQuestion);


    }

    public void CheckFileIsCreate()
    {
        if (File.Exists(DBFilePath) == false)
        {
            var file = File.Create(DBFilePath);
            file.Close();
        }
    }
}
