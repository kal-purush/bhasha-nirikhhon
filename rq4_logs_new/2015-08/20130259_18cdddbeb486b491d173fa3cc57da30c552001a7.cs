using System;

namespace BahnEditor.BahnLib
{
	/// <summary>
	/// The exception is thrown when a layer of a graphic is empty (transparent)
	/// </summary>
	[Serializable]
	public class LayerIsEmptyException : Exception
	{
		public LayerIsEmptyException() { }
		public LayerIsEmptyException(string message) : base(message) { }
		public LayerIsEmptyException(string message, Exception inner) : base(message, inner) { }
		protected LayerIsEmptyException(
		  System.Runtime.Serialization.SerializationInfo info,
		  System.Runtime.Serialization.StreamingContext context)
			: base(info, context) { }
	}
}