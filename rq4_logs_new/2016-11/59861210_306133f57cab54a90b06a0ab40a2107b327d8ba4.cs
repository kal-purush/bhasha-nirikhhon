/*
 * Author(s): Isaiah Mann
 * Description: 
 */

[System.Serializable]
public class IngredientDescriptor : HECData {
	public string Ingredient;
	public string PantryType;
	public string TasteStrength;
	public int TasteHeat;
	public int TasteMoisture;
	public string IncomeQuantity;
	public string Spoilage;
	public string[] Processes;
	public bool ResultsInDish;
	public string[] PositiveExecutionModifiers;
	public string[] NegativeExecutionModifiers;
}

[System.Serializable]
public class IngredientDescriptorList : HECDataList<IngredientDescriptor> {}