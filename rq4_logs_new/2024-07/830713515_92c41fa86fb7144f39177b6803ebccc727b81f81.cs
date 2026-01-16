using System.Text.RegularExpressions;
public class FileEditor : IEditor{
    public string ToUpper(string text)
    {
        var uppered = text.ToUpper();
        return uppered;
    }
    
    public List<string> Split(string text)
    {

        var splittedText = text.Split(" ").ToList();
        return splittedText;
    }
    public string RemoveExtraSpace(string str)
    {
        var myString = Regex.Replace(str, @"\s+", " ");
        return myString;
    }

    public List<string> DeleteStopWord(List<string> word)
    {
        return null;
    }

}