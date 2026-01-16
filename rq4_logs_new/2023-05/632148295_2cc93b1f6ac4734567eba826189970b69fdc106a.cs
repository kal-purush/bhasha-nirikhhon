using Pollaris._3.Accessors;
using Pollaris.Models;

namespace Pollaris.Managers
{
    public class OptionManager
    {
        public void MakeOptionCorrect(int optionId, bool isCorrect)
        {
            SQLAccessor sql = new SQLAccessor();
            sql.ChangeOptionCorrect(optionId, isCorrect); 
        }

        public void ChangeOptionName(int optionId, string newName)
        {
            SQLAccessor sql = new SQLAccessor(); 
            sql.ChangeOptionName(optionId, newName);
        }
    }
}