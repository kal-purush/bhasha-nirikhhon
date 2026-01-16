using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Reflection;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.ComponentModel;
using Windows.Data.Json;
using Newtonsoft.Json;
using System.Drawing;
using System.Globalization;
using Production.Enum;

namespace Production.Model
{
    public static class EffectFactory
    {
        public static void Test(this IEffect effect)
        {
            // TO JSON
            var jsonObj = effect.ToJsonObject();

            // TO INSTANCE
            var e = EffectFactory.CreatInstance(jsonObj.GetNamedString("Name"), jsonObj.GetNamedArray("Parameters").Select(i =>
            {
                var x = i;
                return i.GetString();
            }).ToArray());
        }

        public static IEffect CreatInstance(string name, string[] parameters)
        {
            object[] instanceParams = parameters.Select(i =>
            {
                var param = i.Split(new char[] { '：' }, 2);
                return JsonConvert.DeserializeObject(param[1], Type.GetType(param[0]));
            }).ToArray();

            Type instance = Type.GetType(name);
            return (IEffect)Activator.CreateInstance(instance, instanceParams, new object[] { });
        }

        public static JsonArray ExportToObject(IEnumerable<IEffect> effects)
        {
            JsonArray effectsArray = new JsonArray();

            foreach (var eff in effects)
            {
                effectsArray.Add(eff.ToJsonObject());
            }

            return effectsArray;
        }

        public static JsonObject ToJsonObject(this IEffect effect)
        {
            Type type = effect.GetType();
            JsonObject result = new JsonObject();
            result.Add("Name", JsonValue.CreateStringValue(type.ToString()));

            var constructors = type.GetConstructors();
            var constructor = constructors.Where(i => !i.GetParameters().Any(p => {
                var property = type.GetProperty(p.Name);
                if (property == null) return true;
                return property.GetValue(effect) == null;
            })).FirstOrDefault(); // 取得能建構 Instance 的 Constructor

            JsonArray parameters = new JsonArray();

            var x = constructor.GetParameters();
            foreach(var param in constructor.GetParameters())
            {
                object value = type.GetProperty(param.Name).GetValue(effect);
                parameters.Add(JsonValue.CreateStringValue($"{value.GetType().ToString()}：{JsonConvert.SerializeObject(value)}"));
            }
            result.Add("Parameters", parameters);

            return result;
        }
    }
}