using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AliChoixServer.Model
{
    public class AliChoixProduct
    {
        public string UniversalProductCode { get; set; }

        public string Name { get; set; }

        public Uri ImageUrl { get; set; }

        public string NutriScore { get; set; }

        public string EcoScore { get; set; }

        public NutritionFacts Label { get; set; }

        public List<string> Ingredient { get; set; }

        public AliChoixProduct(OffMongoDbProduct OffProduct)
        {
            UniversalProductCode = OffProduct.UniversalProductCode;
            Name = OffProduct.Name;
            ImageUrl = OffProduct.ImageUrl;
            NutriScore = OffProduct.NutriScore;
            EcoScore = OffProduct.EcoScore;
            Label = new NutritionFacts(OffProduct.Nutriments, OffProduct.ServingQuantity);
            Ingredient = OffProduct.Ingredients.Select(x => x.IngredientName.Trim('_')).ToList();
        }
    }

    public class NutritionFacts
    {
        public Value Serving { get; set; }

        public Value Calorie { get; set; }

        public NutritionFactsRow Fat { get; set; }

        public NutritionFactsRow SaturatedFat { get; set; }

        public NutritionFactsRow TransFat { get; set; }

        public NutritionFactsRow PolyUnsaturatedFat { get; set; }

        public NutritionFactsRow MonoUnsaturatedFat { get; set; }

        public NutritionFactsRow Cholesterol { get; set; }

        public NutritionFactsRow Sodium { get; set; }

        public NutritionFactsRow Carbohydrate { get; set; }

        public NutritionFactsRow Fiber { get; set; }

        public NutritionFactsRow Sugar { get; set; }

        public NutritionFactsRow Protein { get; set; }

        public List<NutritionFactsRow> Nutrients { get; set; }

        public NutritionFacts
        (
            Value serving,
            Value calorie,
            NutritionFactsRow fat,
            NutritionFactsRow saturatedFat,
            NutritionFactsRow transFat,
            NutritionFactsRow polyUnsaturatedFat,
            NutritionFactsRow monoUnsaturatedFat,
            NutritionFactsRow cholesterol,
            NutritionFactsRow sodium,
            NutritionFactsRow carbohydrate,
            NutritionFactsRow fiber,
            NutritionFactsRow sugar,
            NutritionFactsRow protein,
            List<NutritionFactsRow> nutrients
        )
        {
            Serving = serving;
            Calorie = calorie;
            Fat = fat;
            SaturatedFat = saturatedFat;
            TransFat = transFat;
            PolyUnsaturatedFat = polyUnsaturatedFat;
            MonoUnsaturatedFat = monoUnsaturatedFat;
            Cholesterol = cholesterol;
            Sodium = sodium;
            Carbohydrate = carbohydrate;
            Fiber = fiber;
            Sugar = sugar;
            Protein = protein;
            Nutrients = nutrients;             
        }

        public NutritionFacts(OffNutriments nutriments, double servingInGram)
        {
            Serving = servingInGram == 0 ? new Value(100, "g") : new Value(servingInGram, "g");
            Calorie = new Value(nutriments.EnergyValue,nutriments.EnergyUnit);
            Fat = new NutritionFactsRow("Total Fat", new NutrimentValue(nutriments.FatServing, nutriments.FatValue,nutriments.FatUnit), null);
            SaturatedFat = new NutritionFactsRow("Saturated Fat", new NutrimentValue(nutriments.SaturatedFatServing, nutriments.SaturatedFatValue, nutriments.SaturatedFatUnit), null);
            TransFat = new NutritionFactsRow("Trans Fat", new NutrimentValue(nutriments.TransFatServing, nutriments.TransFatValue, nutriments.TransFatUnit), null);
            PolyUnsaturatedFat = new NutritionFactsRow("Polyunsaturated Fat", new NutrimentValue(nutriments.PolyunsaturatedFatServing, nutriments.PolyunsaturatedFatValue, nutriments.PolyunsaturatedFatUnit), null);
            MonoUnsaturatedFat = new NutritionFactsRow("Monounsaturated Fat", new NutrimentValue(nutriments.MonounsaturatedFatServing, nutriments.MonounsaturatedFatValue, nutriments.MonounsaturatedFatUnit), null);
            Cholesterol = new NutritionFactsRow("Cholesterol", new NutrimentValue(nutriments.CholesterolServing, nutriments.CholesterolValue, nutriments.CholesterolUnit), null);
            Sodium = new NutritionFactsRow("Sodium", new NutrimentValue(nutriments.SodiumServing, nutriments.SodiumValue, nutriments.SodiumUnit), null);
            Carbohydrate = new NutritionFactsRow("Carbohydrate", new NutrimentValue(nutriments.CarbohydratesServing, nutriments.CarbohydratesValue, nutriments.CarbohydratesUnit), null);
            Fiber = new NutritionFactsRow("Dietary Fiber", new NutrimentValue(nutriments.FiberServing, nutriments.FiberValue, nutriments.FiberUnit), null);
            Sugar = new NutritionFactsRow("Sugars", new NutrimentValue(nutriments.SugarsServing, nutriments.SugarsValue, nutriments.SugarsUnit), null);
            Protein = new NutritionFactsRow("Protein", new NutrimentValue(nutriments.ProteinsServing, nutriments.ProteinsValue, nutriments.ProteinsUnit), null);
            Nutrients = GetAllOptinalNutriment(nutriments);
        }

        private List<NutritionFactsRow> GetAllOptinalNutriment(OffNutriments nutriments)
        {
            var optinalNutriments = new List<NutritionFactsRow>();

            if(nutriments.VitaminEValue > 0)
            {
                optinalNutriments.Add(new NutritionFactsRow("Vitamin E", new NutrimentValue(nutriments.VitaminEServing, nutriments.VitaminEValue, nutriments.VitaminEUnit), null));
            }
            if (nutriments.VitaminCValue > 0)
            {
                optinalNutriments.Add(new NutritionFactsRow("Vitamin C", new NutrimentValue(nutriments.VitaminCServing, nutriments.VitaminCValue, nutriments.VitaminCUnit), null));
            }
            if (nutriments.VitaminAValue > 0)
            {
                optinalNutriments.Add(new NutritionFactsRow("Vitamin A", new NutrimentValue(nutriments.VitaminAServing, nutriments.VitaminAValue, nutriments.VitaminAUnit), null));
            }

            return optinalNutriments;
        }
    }


    public class NutritionFactsRow
    {
        public string Name { get; set; }

        public NutrimentValue NutrimentValue { get; set; }

        public int? DailyValue { get; set; }

        public NutritionFactsRow(string name, NutrimentValue value, int? dailyValue)
        {
            Name = name;
            NutrimentValue = value;
            DailyValue = dailyValue;
        }
    }

    public class NutrimentValue
    {
        public Value QuantityPerServing { get; set; }

        public Value QuantityPer100G { get; set; }

        public NutrimentValue(Value quantityPerServing, Value quantityPer100G)
        {
            QuantityPerServing = quantityPerServing;
            QuantityPer100G = quantityPer100G;
        }

        public NutrimentValue(double quantityPerServing, double quantityPer100G,  string quantity100GUnit)
        {
            QuantityPerServing = new Value(quantityPerServing, "g");
            QuantityPer100G = new Value(quantityPer100G, quantity100GUnit);
        }

        public double GetEstimatedServingInGram()
        {
            return QuantityPer100G.Quantity / QuantityPerServing.Quantity * 100.0;
        }
    }

    public class Value
    {
        public double Quantity { get; set; }

        public string Unit { get; set; }

        public Value(double quantity, string unit)
        {
            Quantity = quantity;
            Unit = unit;
        }
    }
}
