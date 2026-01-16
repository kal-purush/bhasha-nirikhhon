using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.Serialization;
using System.Web.Script.Serialization;
using AppmetrCS.Actions;
using AppmetrCS.Persister;

namespace AppmetrCS.Serializations
{
    public class JavaScriptJsonSerializerWithCache : IJsonSerializer
    {
        public static readonly JavaScriptJsonSerializerWithCache Instance = new JavaScriptJsonSerializerWithCache();
        
        private static readonly JavaScriptSerializer Serializer;

        static JavaScriptJsonSerializerWithCache()
        {
            Serializer = new JavaScriptSerializer();
            Serializer.MaxJsonLength = 20 * 1024 * 1024; // 20 MB
            Serializer.RegisterConverters(new[] { new BatchJsonConverter() });
        }

        public String Serialize(Object obj)
        {
            var json = Serializer.Serialize(obj);
            return json;
        }

        public T Deserialize<T>(String json)
        {
            var result = Serializer.Deserialize<T>(json);
            return result;
        }

        /// <summary>
        /// If you want to add new Object types for this serializer, you should add this type to <see cref="SupportedTypes"/>, and write a little bit of code in <see cref="ConvertDictionaryToObject"/> method
        /// </summary>
        internal class BatchJsonConverter : JavaScriptConverter
        {
            private const String TypeFieldName = "___type";
            //We couldn't use __ prefix, cause this prefix are used for DataContractSerializer and Deserialize method throw Exception

            private static readonly Dictionary<Type, TypeDescription> _typeDescriptions
                = new Dictionary<Type, TypeDescription>();

            public override Object Deserialize(
                IDictionary<String, Object> dictionary,
                Type type,
                JavaScriptSerializer serializer)
            {
                return ConvertDictionaryToObject(dictionary);
            }

            public override IDictionary<String, Object> Serialize(Object obj, JavaScriptSerializer serializer)
            {
                if (ReferenceEquals(obj, null)) return null;

                var objType = obj.GetType();
                var typeDescription = GetTypeDescription(objType);
                if (!typeDescription.Serializable) return null;

                var result = new Dictionary<String, Object> { { TypeFieldName, objType.AssemblyQualifiedName } };

                ProcessFieldsAndProperties(typeDescription,
                    (name, info) =>
                    {
                        var fieldInfo = (FieldInfo) info;
                        var value = fieldInfo.GetValue(obj);
                        if (value == null) return;
                        result.Add(name, value);
                    },
                    (name, info) =>
                    {
                        var propertyInfo = (PropertyInfo) info;
                        var value = propertyInfo.GetValue(obj, null);
                        if (value == null) return;
                        result.Add(name, value);
                    });

                return result;
            }

            public override IEnumerable<Type> SupportedTypes => new[] { typeof(Batch), typeof(AppMetrAction) };

            private static Object ConvertDictionaryToObject(IDictionary<String, Object> dictionary)
            {
                var objType = GetSerializedObjectType(dictionary);
                if (objType == null) return null;

                var constructor =
                    objType.GetConstructor(BindingFlags.Instance | BindingFlags.NonPublic, null, new Type[0], null) ??
                    objType.GetConstructor(BindingFlags.Instance | BindingFlags.Public, null, new Type[0], null);

                var result = constructor.Invoke(null);

                Action<String, MemberInfo> action = (name, info) =>
                {
                    var fieldType = info is FieldInfo
                        ? (info as FieldInfo).FieldType
                        : info is PropertyInfo ? (info as PropertyInfo).PropertyType : null;

                    var setValue = info.GetType().GetMethod("SetValue", new[] { typeof(object), typeof(object) });

                    if (fieldType == null || setValue == null) return;

                    var value = GetValue(dictionary, name);
                    if (typeof(ICollection<AppMetrAction>).IsAssignableFrom(fieldType))
                    {
                        var serializedActions = value as ArrayList;
                        if (serializedActions != null)
                        {
                            var actions = (ICollection<AppMetrAction>)Activator.CreateInstance(fieldType);
                            foreach (var val in serializedActions)
                            {
                                if (val is IDictionary<string, object>)
                                    actions.Add(
                                        (AppMetrAction)
                                        ConvertDictionaryToObject(val as IDictionary<string, object>));
                            }
                            setValue.Invoke(info, new[] { result, actions });
                        }
                    }
                    else
                    {
                        setValue.Invoke(info, new[] { result, value });
                    }
                };

                var typeDescription = GetTypeDescription(result.GetType());
                ProcessFieldsAndProperties(typeDescription, action, action);

                return result;
            }

            private static Type GetSerializedObjectType(IDictionary<string, object> dictionary)
            {
                Object typeName;
                if (!dictionary.TryGetValue(TypeFieldName, out typeName) || !(typeName is string))
                    return null;

                return Type.GetType(typeName as string);
            }

            private static Object GetValue(IDictionary<string, object> dictionary, string key)
            {
                Object value;
                dictionary.TryGetValue(key, out value);

                return value;
            }

            private static void ProcessFieldsAndProperties(TypeDescription typeDescription,
                Action<String, MemberInfo> fieldProcessor,
                Action<String, MemberInfo> propertiesProcessor)
            {
                foreach (var field in typeDescription.Fields)
                {
                    fieldProcessor(field.Name, field.Field);
                }

                foreach (var property in typeDescription.Properities)
                {
                    propertiesProcessor(property.Name, property.Property);
                }
            }

            private static TypeDescription GetTypeDescription(Type type)
            {               
                TypeDescription result;
                lock (_typeDescriptions)
                {
                    if (_typeDescriptions.TryGetValue(type, out result)) return result;
                }

                const BindingFlags bindingFlags =
                    BindingFlags.Instance | BindingFlags.FlattenHierarchy | BindingFlags.Public |
                    BindingFlags.NonPublic;

                if (Attribute.GetCustomAttribute(type, typeof (DataContractAttribute)) == null)
                {
                    result = new TypeDescription(
                        false,
                        new SerializableField[0],
                        new SerializableProperty[0]);

                    lock (_typeDescriptions)
                    {
                        _typeDescriptions[type] = result;
                    }
                    return result;
                }

                var fields = new List<SerializableField>();
                var properties = new List<SerializableProperty>();

                var objType = type;
                while (objType != null && objType != typeof(object))
                {
                    foreach (var field in objType.GetFields(bindingFlags))
                    {
                        var attribute = (DataMemberAttribute) JavaScriptJsonSerializer.GetCustomAttribute(field, typeof(DataMemberAttribute));
                        if (attribute != null) fields.Add(new SerializableField(attribute.Name, field));
                    }

                    foreach (var property in objType.GetProperties(bindingFlags))
                    {
                        var attribute = (DataMemberAttribute)JavaScriptJsonSerializer.GetCustomAttribute(property, typeof(DataMemberAttribute));
                        if (attribute != null) properties.Add(new SerializableProperty(attribute.Name, property));
                    }

                    objType = objType.BaseType;
                }

                result = new TypeDescription(
                    fields.Count > 0 || properties.Count > 0,
                    fields.ToArray(),
                    properties.ToArray());

                lock (_typeDescriptions) {
                    _typeDescriptions[type] = result;
                }
                return result;
            }
        }

        private struct TypeDescription
        {
            public readonly Boolean Serializable;
            public readonly SerializableField[] Fields;
            public readonly SerializableProperty[] Properities;

            public TypeDescription(Boolean serializable, SerializableField[] fields, SerializableProperty[] properities)
            {
                Serializable = serializable;
                Fields = fields;
                Properities = properities;
            }
        }

        private struct SerializableField
        {
            public readonly String Name;
            public readonly FieldInfo Field;

            public SerializableField(String name, FieldInfo field)
            {
                Name = name;
                Field = field;
            }
        }

        private struct SerializableProperty
        {
            public readonly String Name;
            public readonly PropertyInfo Property;

            public SerializableProperty(String name, PropertyInfo property)
            {
                Name = name;
                Property = property;
            }
        }
    }
}