using System.Collections.Generic;

namespace FridgeWPF
{
     sealed class FactoryPicker //SINGLETON przechowuje listę dostępnych, zaprogramowanych fabryk 
        {
            public List<IngredientFactory> listOfFactories;
            private static FactoryPicker instance;

            private FactoryPicker()
            {
                listOfFactories = new List<IngredientFactory>();
                listOfFactories.Add(new MilkFactory());
            }
            public static FactoryPicker Instance
            {
                get
                {
                    if(instance == null)
                    {
                        instance = new FactoryPicker();
                    }
                    return instance;
                }
            }

            public IngredientFactory Pick(string ingredientName) //dobiera fabrykę na podstawie jej nazwy zakodowanej w bazie danych
            {
                IngredientFactory pickedFactory=null;
                foreach(IngredientFactory IF in listOfFactories)
                {
                    if (ingredientName == IF.name)
                    {
                        pickedFactory = IF;
                    }
                }
                return pickedFactory;
            }
    }
}