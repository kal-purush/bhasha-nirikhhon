using System;
using System.IO;
using System.Collections.Generic;
namespace GXPEngine
{
	public class Layer
	{
		private string[,] _layerArray;
		private string _filename;
		public Layer( string filename )
		{
			try
			{
				_filename = filename;
			}
			catch
			{
				Console.WriteLine( "File not found!" );
			}
		}

		public string[,] CreateLayer()
		{

			StreamReader sr = new StreamReader( _filename );
			var lines = new List<string[]>();
			int row = 0;
			while (!sr.EndOfStream)
			{
				string[] Line = sr.ReadLine().Split( ',' );
				lines.Add( Line );
				row++;
			}

			var data = lines.ToArray();
			_layerArray = new string[data.GetLength( 0 ), data.GetLength( 1 )];
			return _layerArray;
		}

		public int GetLayerWidth { get { return _layerArray.GetLength( 0 ); } }
		public int GetLayerHeight { get { return _layerArray.GetLength( 1 ); } }
		public string[,] GetLayerArray {get{ return _layerArray; } }
		/// <summary>
		/// Returns the designatd value of the layer on the parsed x and y values
		/// </summary>
		/// <returns>The layer item.</returns>
		/// <param name="x">The x coordinate.</param>
		/// <param name="y">The y coordinate.</param>
		public string GetLayerValue(int x, int y) 
		{
			return _layerArray[x, y];
		}
	}
}
