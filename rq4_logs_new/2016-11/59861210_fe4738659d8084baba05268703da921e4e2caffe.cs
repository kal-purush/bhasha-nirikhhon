/*
 * Author(s): Isaiah Mann
 * Description: 
 */

[System.Serializable]
public class ProcessDescriptor : HECData {
	public string Process;
	public string Type;
	public string Station;
	public int TasteHeat;
	public int TasteMoisture;
	public string[] Ingredients;
	public bool Active;
	public string[] Obsoletes;
	public string [] ExecutionBonuses;
	public string [] ExecutionPenalties;
	public int IdealTimeMin;
	public int IdealTimeMax;
	public string ProgressBarPNG;


	public override string ToString () {
		return string.Format ("[ProcessDescriptor] " +
			"{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}",
			Process,
			Type,
			Station,
			TasteHeat,
			TasteMoisture,
			ArrayUtil.ToString(Ingredients),
			Active,
			ArrayUtil.ToString(Obsoletes),
			IdealTimeMin,
			IdealTimeMax,
			ProgressBarPNG
		);
	}
}

[System.Serializable]
public class ProcessDescriptorList : HECDataList<ProcessDescriptor> {}