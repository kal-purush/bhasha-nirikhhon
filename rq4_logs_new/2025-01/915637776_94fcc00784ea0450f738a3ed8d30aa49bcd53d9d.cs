
[Icon( "shirt" )]
public struct ClothesPlanner
{
	[Property]
	public List<Clothing> Choiches { get; set; }

	/// <summary>
	/// 1x = as many chances as other choices, 2x twice as many
	/// </summary>
	[Property]
	[Range( 0f, 10f, 0.1f )]
	public float EmptyWeight { get; set; }

	public Clothing Random()
	{
		Dictionary<Clothing, float> weightedDictionary = Choiches.ToDictionary( key => key, value => 1f );

		if ( EmptyWeight > 0f )
			weightedDictionary.Add( null, EmptyWeight );

		return WeightedList.RandomKey<Clothing>( weightedDictionary );
	}
}