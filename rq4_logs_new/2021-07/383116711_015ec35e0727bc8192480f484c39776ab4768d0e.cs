using AnagramSolver.EF.DatabaseFirst.Models;
using System.Linq;

namespace AnagramSolver.BusinessLogic.Classes.Users
{
    public class UserRepository
    {
        public Word ConnectWithDb(string word)
        {
            using (var context = new VocabularyDBContext())
            {
                var wordFromDB = context.Words.FirstOrDefault(w => w.Word1 == word);
                return wordFromDB;
            }

        }
        public void UploadToDb()
        {
            using (var context = new VocabularyDBContext())
            {
                var userToAdd = new User { FirstName = model.FirstName, LastName = model.LastName, Email = model.Email, Pass = pass };
                context.Users.Add(userToAdd);
                context.SaveChanges();
            }
        }
    }
}