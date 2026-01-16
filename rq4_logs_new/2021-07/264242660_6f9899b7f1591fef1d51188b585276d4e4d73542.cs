using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using UnityEngine;
using UnityEditor;
using UnityEngine.UI;

public class UIClass : MonoBehaviour
{
    [MenuItem("Veauty/GenerateUIClass")]
    static void GenerateUIClass()
    {
        var assm = Assembly.GetAssembly(typeof(Button));
        var types = assm.GetTypes()
                        .Where(x => x.Namespace == "UnityEngine.UI")
                        .Where(x => x.IsSubclassOf(typeof(MonoBehaviour)))
                        .Where(x => x.Name != "DrowdownItem");

        types.ToList()
             .ForEach(x => {
                 var properties = x.GetProperties(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance).Where(x => x.CanWrite);
                 var code = GUIScriptTemplate(x, properties);
                 CreateScript(x.Name, code);
             });
        
        AssetDatabase.Refresh();
    }

    static void CreateScript(string name, string code)
    {
        var filePath = $"Packages/Veauty-uGUI/Lib/Generated/{name}.cs";

        System.IO.File.WriteAllText(filePath, code);
    }

    static string GUIScriptTemplate(System.Type uGUIType, IEnumerable<PropertyInfo> propertyTypes) =>
        $@"
// THIS CODE IS AUTO GENERATED

using UnityEngine;
using UnityEngine.Events;
using Veauty.GameObject.Attributes;
using UI = UnityEngine.UI;
using Veauty.VTree;
using System.Collections.Generic;

namespace Veauty.uGUI
{{
    public abstract class {uGUIType.Name}Attribute<T> : GuiAttributeBase<{TypeString(uGUIType)}, T>
    {{
        protected {uGUIType.Name}Attribute(string key, T value) : base(key, value) {{ }}
    }}

    public class {uGUIType.Name} : GUIBase<{TypeString(uGUIType)}>
    {{
        public {uGUIType.Name}(IEnumerable<IAttribute<UnityEngine.GameObject>> attrs, params IVTree[] kids) : base(attrs, kids) {{ }}

        public override UnityEngine.GameObject Init(UnityEngine.GameObject go)
        {{
            return go;
        }}
        public override void Destroy(UnityEngine.GameObject go) {{ }}

        {propertyTypes.Select(x => UIPropertyClassTemplate(uGUIType, x)).Aggregate("", (acc, x) => acc + $"{x}\n")}
    }}
}}";

    static string UIPropertyClassTemplate(System.Type baseClass, PropertyInfo property) =>
        $@"
        public class {ToPascalCase(property.Name)} : {baseClass.Name}Attribute<{TypeString(property.PropertyType)}>
        {{
            public {ToPascalCase(property.Name)}({TypeString(property.PropertyType)} value): base(""{property.Name}"", value) {{}}
            protected override void Apply({TypeString(baseClass)} component)
            {{
                component.{property.Name} = this.GetValue();
            }}
        }}";

    static string ToPascalCase(string str)
    {
        var regex = new System.Text.RegularExpressions.Regex("(.)(.*)");
        return regex.Replace(str, s => $"{s.Groups[1].Value.ToUpper()}{s.Groups[2].Value}");
    }

    static string TypeString(System.Type type)
    {
        if (type.IsGenericType)
        {
            var genericTypes = type.GetGenericArguments().Select(x => TypeString(x)).Aggregate((acc, x) => acc + $", {x}");
            var regex = new System.Text.RegularExpressions.Regex("(.+)(`[0-9])(.*)");
            return regex.Replace(type.FullName.Replace('+', '.'), s => $"{s.Groups[1].Value}<{genericTypes}>");
        }

        return type.FullName.Replace('+', '.');
    }

}