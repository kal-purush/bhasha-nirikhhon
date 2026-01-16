using Sandbox;
using System.Text.Json.Serialization;

public struct ChartDataSets
{
	[JsonPropertyName( "backgroundColor" )]
	public string[] BackgroundColor { get; set; }

	[JsonPropertyName( "data" )]
	public int[] Data { get; set; }
}

public struct ChartData
{
	[JsonPropertyName( "labels" )]
	public string[] Labels { get; set; }

	[JsonPropertyName( "datasets" )]
	public ChartDataSets[] DataSets { get; set; }

}

public struct ChartFont
{
	[JsonPropertyName( "resizable" )]
	public bool Resizable { get; set; }

	[JsonPropertyName( "minSize" )]
	public int MinSize { get; set; }

	[JsonPropertyName( "maxSize" )]
	public int MaxSize { get; set; }
}

public struct ChartOutlabels
{
	[JsonPropertyName( "text" )]
	public string Text { get; set; }

	[JsonPropertyName( "color" )]
	public string Color { get; set; }

	[JsonPropertyName( "stretch" )]
	public int Stretch { get; set; }

	[JsonPropertyName( "font" )]
	public ChartFont Font { get; set; }
}

public struct ChartPlugins
{
	[JsonPropertyName( "legend" )]
	public bool Legend { get; set; }

	[JsonPropertyName( "outlabels" )]
	public ChartOutlabels Outlabels { get; set; }
}

public struct ChartOptions
{
	[JsonPropertyName( "plugins" )]
	public ChartPlugins Plugins { get; set; }
}

public struct Chart
{
	[JsonPropertyName( "type" )]
	public string ChartType { get; set; }

	[JsonPropertyName( "data" )]
	public ChartData Date { get; set; }

	[JsonPropertyName( "options" )]
	public ChartOptions Options { get; set; }
}