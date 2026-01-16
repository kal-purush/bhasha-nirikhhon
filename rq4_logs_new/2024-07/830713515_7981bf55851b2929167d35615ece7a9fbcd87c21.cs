public interface IQueryFormatter
{
    public string ToUpper(string text);
    public List<string> Split(string text);
    public string RemoveExtraSpace(string str);
    public List<string> SeparatePrefix(List<string> input);
    public List<string> ExtractBySign(List<string> list, char c);


}