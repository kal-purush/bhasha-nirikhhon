using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static FridgeWPF.MainWindow;

namespace FridgeWPF
{
    public abstract class AbstractFridge
    {
        public FridgeFiller Filler;
        private NewIngredientSaver NewIngredient;
        List<AbstractIngredient> Content= new List<AbstractIngredient>();

        public AbstractFridge(MainWindow window)
        {
            Filler = new FridgeFiller(this, window);
        }

        public List<AbstractIngredient> GetContent()
        {
            return Content;
        }

        public void AddIngredient(AbstractIngredient ingredient)
        {
            Content.Add(ingredient);
        }

        public void AddNewIngredientToDatabase(OnlineDataBase dataBase, string ingredientName, double amount, DateTime expiryDate)
        {
            NewIngredient = new NewIngredientSaver(dataBase);
            NewIngredient.AddNewIngredient(ingredientName, amount, expiryDate);
        }

        public void DeleteIngredientFromDataBase(OnlineDataBase dataBase, AbstractIngredient ingredient)
        {
            dataBase.DeleteIngredientFromDatabase(ingredient);
        }
    }
}