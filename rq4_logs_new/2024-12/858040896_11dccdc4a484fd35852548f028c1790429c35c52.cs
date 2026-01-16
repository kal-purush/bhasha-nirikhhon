using System.Reflection;
using System.Text;

namespace ReflectorApp;

/// <summary>
/// Класс для анализа структуры классов и их сравнения.
/// </summary>
public class Reflector
{
    /// <summary>
    /// Создает файл с описанием структуры указанного класса.
    /// </summary>
    /// <param name="someClass">Тип анализируемого класса.</param>
    public void PrintStructure(Type someClass)
    {
        var fileName = $"{someClass.Name}.cs";
        using var writer = new StreamWriter(fileName, false, Encoding.UTF8);

        writer.WriteLine($"public class {someClass.Name}");
        writer.WriteLine("{");

        foreach (var field in someClass.GetFields(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
        {
            string visibility = GetVisibility(field);
            string staticModifier = field.IsStatic ? "static " : "";
            writer.WriteLine($"    {visibility} {staticModifier}{field.FieldType.Name} {field.Name};");
        }

        foreach (var method in someClass.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (method.IsSpecialName) continue;
            string visibility = GetVisibility(method);
            string staticModifier = method.IsStatic ? "static " : "";
            var parameters = string.Join(", ", method.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"));
            writer.WriteLine($"    {visibility} {staticModifier}{method.ReturnType.Name} {method.Name}({parameters});");
        }

        writer.WriteLine("}");
    }

    /// <summary>
    /// Выводит различия между двумя классами: поля и методы, присутствующие только в одном из них.
    /// </summary>
    /// <param name="a">Тип первого класса.</param>
    /// <param name="b">Тип второго класса.</param>
    public void DiffClasses(Type a, Type b)
    {
        var fieldsA = a.GetFields(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        var fieldsB = b.GetFields(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

        Console.WriteLine("Fields differences:");
        foreach (var field in fieldsA.Except(fieldsB, new MemberInfoComparer()))
        {
            Console.WriteLine($"Only in {a.Name}: {field.Name}");
        }
        foreach (var field in fieldsB.Except(fieldsA, new MemberInfoComparer()))
        {
            Console.WriteLine($"Only in {b.Name}: {field.Name}");
        }

        var methodsA = a.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic).Where(m => !m.IsSpecialName);
        var methodsB = b.GetMethods(BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic).Where(m => !m.IsSpecialName);

        Console.WriteLine("Methods differences:");
        foreach (var method in methodsA.Except(methodsB, new MemberInfoComparer()))
        {
            Console.WriteLine($"Only in {a.Name}: {method.Name}");
        }
        foreach (var method in methodsB.Except(methodsA, new MemberInfoComparer()))
        {
            Console.WriteLine($"Only in {b.Name}: {method.Name}");
        }
    }

    private string GetVisibility(MemberInfo member)
    {
        return member switch
        {
            FieldInfo field => field.IsPublic ? "public" : field.IsPrivate ? "private" : "protected",
            MethodInfo method => method.IsPublic ? "public" : method.IsPrivate ? "private" : "protected",
            _ => "private"
        };
    }

    private class MemberInfoComparer : IEqualityComparer<MemberInfo>
    {
        public bool Equals(MemberInfo x, MemberInfo y) => x?.Name == y?.Name && x?.MemberType == y?.MemberType;
        public int GetHashCode(MemberInfo obj) => obj.Name.GetHashCode() ^ obj.MemberType.GetHashCode();
    }
}