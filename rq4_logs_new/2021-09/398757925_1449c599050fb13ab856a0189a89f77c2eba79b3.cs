using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Xml;

using InternalXmlSerializer = System.Xml.Serialization.XmlSerializer;

namespace Ralfred.Common.Helpers.Serialization
{
	public class XmlSerializer : ISerializer
	{
		public string? Serialize(object? @object)
		{
			if (@object is null)
				return null;

			var result = new StringBuilder();
			var serializer = new InternalXmlSerializer(@object.GetType());

			using var writer = XmlWriter.Create(result);
			serializer.Serialize(writer, @object);

			return result.ToString();
		}

		public T Deserialize<T>(string? serializedObject) where T : class
		{
			if (serializedObject is null)
				return default;

			var serializer = new InternalXmlSerializer(typeof(T));

			using var memoryStream = new MemoryStream();
			using var writer = new StreamWriter(memoryStream);

			writer.Write(serializedObject);
			writer.Flush();

			memoryStream.Position = 0;

			using var xmlReader = XmlReader.Create(memoryStream);

			return (T) serializer.Deserialize(xmlReader)!;
		}
	}
}