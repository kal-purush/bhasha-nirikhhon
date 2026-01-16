using Newtonsoft.Json;

class UserStorage
{

    public string FileDBName;
    public string FileFolderPath;
    public string DBFilePath;


    public void ClearDB()
    {

        File.WriteAllText(DBFilePath, "");
        Console.WriteLine("Готово");

    }


    public void SaveToDB(List<User> users)
    {

        string serializedUsers = JsonConvert.SerializeObject(users);
        File.WriteAllText(DBFilePath, serializedUsers);

    }

    public List<User> ReadAllFromDB()
    {

        string json = File.ReadAllText(DBFilePath);
        List<User> currentUsers = JsonConvert.DeserializeObject<List<User>>(json);


        return currentUsers ?? new List<User>();

    }


    public void SaveToDB(User user)
    {
        List<User> AllCurrentUsers = ReadAllFromDB();
        int lastId = AllCurrentUsers.Count == 0 ? 0 : AllCurrentUsers.Last().Id;

        user.SetNewId(lastId + 1);

        AllCurrentUsers.Add(user);
        string serializedUsers = JsonConvert.SerializeObject(AllCurrentUsers);
        File.WriteAllText(DBFilePath, serializedUsers);


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







