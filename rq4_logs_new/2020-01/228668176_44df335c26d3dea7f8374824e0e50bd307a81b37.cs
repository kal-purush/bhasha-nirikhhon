/*
Copyright © 2017-2019 César Andrés Morgan
Licenciado para uso interno solamente.
*/

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using TheXDS.Proteus.Models.Base;
using System.Reflection;
using TheXDS.MCART.Types.Extensions;

namespace TheXDS.Proteus.Component
{
    public class NotResolvedRelationsContractResolver : DefaultContractResolver
    {
        private class NotLinkedValueProvider : IValueProvider
        {
            private readonly string _propName;

            public NotLinkedValueProvider(string propertyName)
            {
                _propName = propertyName;
            }

            public object? GetValue(object target)
            {
                var m = target as ModelBase;
                if (m?.GetType().GetProperty(_propName) is PropertyInfo p)
                {
                    var linked = p.GetValue(m) as ModelBase;
                    return linked?.IdAsObject;
                }
                return null;
            }

            public void SetValue(object target, object value)
            {
                /* No hacer nada. */
            }
        }


        static readonly string[] _ignore = { "children", "isnew", "idtype", "idasobject", "stringid", "isdeleted", "idtype" };

        /// <inheritdoc/>
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var property = base.CreateProperty(member, memberSerialization);

            if (_ignore.Contains(property.PropertyName.ToLower()))
                property.Ignored = true;

            if (property.PropertyType.Implements<ModelBase>())
            {
                property.ValueProvider = new NotLinkedValueProvider(property.PropertyName);
                property.ShouldDeserialize = _ => false;
                property.PropertyName = $"{property.PropertyName}_Id";
            }

            return property;
        }
    }

}