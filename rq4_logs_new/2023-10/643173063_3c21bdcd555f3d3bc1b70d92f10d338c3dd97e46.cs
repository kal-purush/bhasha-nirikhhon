
using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace A2v10.BuildSql;

/*
 * BASED ON
 * https://github.com/zanders3/json/blob/master/src/JSONParser.cs
 */
public class JSONParser
{
	private readonly Stack<List<string>> splitArrayPool = new();
	private readonly StringBuilder stringBuilder = new();
	private readonly Dictionary<Type, Dictionary<string, FieldInfo>> fieldInfoCache = new();
	private readonly Dictionary<Type, Dictionary<string, PropertyInfo>> propertyInfoCache = new();

	public static T? DeserializeObject<T>(String json)
	{
		var prs = new JSONParser();
		return prs.FromJson<T>(json);
	}
	internal T? FromJson<T>(String json)
	{
		//Remove all whitespace not within strings to make parsing simpler
		stringBuilder.Length = 0;
		for (int i = 0; i < json.Length; i++)
		{
			char c = json[i];
			if (c == '"')
			{
				i = AppendUntilStringEnd(true, i, json);
				continue;
			}
			if (char.IsWhiteSpace(c))
				continue;

			stringBuilder.Append(c);
		}

		//Parse the thing!
		return (T?)ParseValue(typeof(T), stringBuilder.ToString());
	}

	int AppendUntilStringEnd(bool appendEscapeCharacter, int startIdx, string json)
	{
		stringBuilder.Append(json[startIdx]);
		for (int i = startIdx + 1; i < json.Length; i++)
		{
			if (json[i] == '\\')
			{
				if (appendEscapeCharacter)
					stringBuilder.Append(json[i]);
				stringBuilder.Append(json[i + 1]);
				i++;//Skip next character as it is escaped
			}
			else if (json[i] == '"')
			{
				stringBuilder.Append(json[i]);
				return i;
			}
			else
				stringBuilder.Append(json[i]);
		}
		return json.Length - 1;
	}

	//Splits { <value>:<value>, <value>:<value> } and [ <value>, <value> ] into a list of <value> strings
	List<string> Split(string json)
	{
		List<string> splitArray = splitArrayPool.Count > 0 ? splitArrayPool.Pop() : new List<string>();
		splitArray.Clear();
		if (json.Length == 2)
			return splitArray;
		int parseDepth = 0;
		stringBuilder.Length = 0;
		for (int i = 1; i < json.Length - 1; i++)
		{
			switch (json[i])
			{
				case '[':
				case '{':
					parseDepth++;
					break;
				case ']':
				case '}':
					parseDepth--;
					break;
				case '"':
					i = AppendUntilStringEnd(true, i, json);
					continue;
				case ',':
				case ':':
					if (parseDepth == 0)
					{
						splitArray.Add(stringBuilder.ToString());
						stringBuilder.Length = 0;
						continue;
					}
					break;
			}

			stringBuilder.Append(json[i]);
		}

		splitArray.Add(stringBuilder.ToString());

		return splitArray;
	}

	internal Object? ParseValue(Type type, String json)
	{
		if (type == typeof(String))
		{
			if (json.Length <= 2)
				return string.Empty;
			StringBuilder parseStringBuilder = new(json.Length);
			for (int i = 1; i < json.Length - 1; ++i)
			{
				if (json[i] == '\\' && i + 1 < json.Length - 1)
				{
					int j = "\"\\nrtbf/".IndexOf(json[i + 1]);
					if (j >= 0)
					{
						parseStringBuilder.Append("\"\\\n\r\t\b\f/"[j]);
						++i;
						continue;
					}
					if (json[i + 1] == 'u' && i + 5 < json.Length - 1)
					{
						if (UInt32.TryParse(json.Substring(i + 2, 4), System.Globalization.NumberStyles.AllowHexSpecifier, null, out UInt32 c))
						{
							parseStringBuilder.Append((char)c);
							i += 5;
							continue;
						}
					}
				}
				parseStringBuilder.Append(json[i]);
			}
			return parseStringBuilder.ToString();
		}
		if (type.IsPrimitive)
		{
			var result = Convert.ChangeType(json, type, System.Globalization.CultureInfo.InvariantCulture);
			return result;
		}
		if (type == typeof(Decimal))
		{
			Decimal.TryParse(json, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out Decimal result);
			return result;
		}
		if (type == typeof(DateTime))
		{
			DateTime.TryParse(json.Replace("\"", ""), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out DateTime result);
			return result;
		}
		if (json == "null")
		{
			return null;
		}
		if (type.IsEnum)
		{
			if (json[0] == '"')
				json = json.Substring(1, json.Length - 2);
			try
			{
				return Enum.Parse(type, json, false);
			}
			catch
			{
				return 0;
			}
		}
		if (type.IsArray)
		{
			Type arrayType = type.GetElementType();
			if (json[0] != '[' || json[json.Length - 1] != ']')
				return null;

			List<string> elems = Split(json);
			Array newArray = Array.CreateInstance(arrayType, elems.Count);
			for (int i = 0; i < elems.Count; i++)
				newArray.SetValue(ParseValue(arrayType, elems[i]), i);
			splitArrayPool.Push(elems);
			return newArray;
		}
		if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(List<>))
		{
			Type listType = type.GetGenericArguments()[0];
			if (json[0] != '[' || json[json.Length - 1] != ']')
				return null;

			List<string> elems = Split(json);
			var list = (IList)type.GetConstructor(new Type[] { typeof(int) }).Invoke(new object[] { elems.Count });
			for (int i = 0; i < elems.Count; i++)
				list.Add(ParseValue(listType, elems[i]));
			splitArrayPool.Push(elems);
			return list;
		}
		if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
		{
			Type keyType, valueType;
			{
				Type[] args = type.GetGenericArguments();
				keyType = args[0];
				valueType = args[1];
			}

			//Refuse to parse dictionary keys that aren't of type string
			if (keyType != typeof(string))
				return null;
			//Must be a valid dictionary element
			if (json[0] != '{' || json[json.Length - 1] != '}')
				return null;
			//The list is split into key/value pairs only, this means the split must be divisible by 2 to be valid JSON
			List<string> elems = Split(json);
			if (elems.Count % 2 != 0)
				return null;

			var dictionary = (IDictionary)type.GetConstructor(new Type[] { typeof(int) }).Invoke(new object[] { elems.Count / 2 });
			for (int i = 0; i < elems.Count; i += 2)
			{
				if (elems[i].Length <= 2)
					continue;
				String keyValue = elems[i].Substring(1, elems[i].Length - 2);
				Object? val = ParseValue(valueType, elems[i + 1]);
				dictionary[keyValue] = val;
			}
			return dictionary;
		}
		if (type == typeof(object))
		{
			return ParseAnonymousValue(json);
		}
		if (json[0] == '{' && json[json.Length - 1] == '}')
		{
			return ParseObject(type, json);
		}

		return null;
	}

	Object? ParseAnonymousValue(String json)
	{
		if (json.Length == 0)
			return null;
		if (json[0] == '{' && json[json.Length - 1] == '}')
		{
			List<string> elems = Split(json);
			if (elems.Count % 2 != 0)
				return null;
			var dict = new Dictionary<String, Object?>(elems.Count / 2);
			for (int i = 0; i < elems.Count; i += 2)
				dict[elems[i].Substring(1, elems[i].Length - 2)] = ParseAnonymousValue(elems[i + 1]);
			return dict;
		}
		if (json[0] == '[' && json[json.Length - 1] == ']')
		{
			List<String> items = Split(json);
			var finalList = new List<Object?>(items.Count);
			for (int i = 0; i < items.Count; i++)
				finalList.Add(ParseAnonymousValue(items[i]));
			return finalList;
		}
		if (json[0] == '"' && json[json.Length - 1] == '"')
		{
			string str = json.Substring(1, json.Length - 2);
			return str.Replace("\\", String.Empty);
		}
		if (char.IsDigit(json[0]) || json[0] == '-')
		{
			if (json.Contains("."))
			{
				Double.TryParse(json, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out Double result);
				return result;
			}
			else
			{
				Int32.TryParse(json, out Int32 result);
				return result;
			}
		}
		if (json == "true")
			return true;
		if (json == "false")
			return false;
		// handles json == "null" as well as invalid JSON
		return null;
	}

	Dictionary<string, T> CreateMemberNameDictionary<T>(T[] members) where T : MemberInfo
	{
		Dictionary<string, T> nameToMember = new(StringComparer.OrdinalIgnoreCase);
		for (int i = 0; i < members.Length; i++)
		{
			T member = members[i];
			if (member.IsDefined(typeof(IgnoreDataMemberAttribute), true))
				continue;

			string name = member.Name;
			if (member.IsDefined(typeof(DataMemberAttribute), true))
			{
				DataMemberAttribute dataMemberAttribute = (DataMemberAttribute)Attribute.GetCustomAttribute(member, typeof(DataMemberAttribute), true);
				if (!string.IsNullOrEmpty(dataMemberAttribute.Name))
					name = dataMemberAttribute.Name;
			}

			nameToMember.Add(name, member);
		}

		return nameToMember;
	}

	object ParseObject(Type type, string json)
	{
		object instance = FormatterServices.GetUninitializedObject(type);

		//The list is split into key/value pairs only, this means the split must be divisible by 2 to be valid JSON
		List<string> elems = Split(json);
		if (elems.Count % 2 != 0)
			return instance;

		if (!fieldInfoCache.TryGetValue(type, out var nameToField))
		{
			nameToField = CreateMemberNameDictionary(type.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy));
			fieldInfoCache.Add(type, nameToField);
		}
		if (!propertyInfoCache.TryGetValue(type, out var nameToProperty))
		{
			nameToProperty = CreateMemberNameDictionary(type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.FlattenHierarchy));
			propertyInfoCache.Add(type, nameToProperty);
		}

		for (int i = 0; i < elems.Count; i += 2)
		{
			if (elems[i].Length <= 2)
				continue;
			string key = elems[i].Substring(1, elems[i].Length - 2);
			string value = elems[i + 1];

			if (nameToField.TryGetValue(key, out FieldInfo fieldInfo))
				fieldInfo.SetValue(instance, ParseValue(fieldInfo.FieldType, value));
			else if (nameToProperty.TryGetValue(key, out PropertyInfo propertyInfo))
				propertyInfo.SetValue(instance, ParseValue(propertyInfo.PropertyType, value), null);
		}

		return instance;
	}
}