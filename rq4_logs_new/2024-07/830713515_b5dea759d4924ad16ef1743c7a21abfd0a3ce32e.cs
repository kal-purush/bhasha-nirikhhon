public class Mapper
{
    public Dictionary<string, List<Document>> Map(List<Document> documentList)
    {
        var tempDic = new Dictionary<string, HashSet<Document>>();

        foreach(var d in documentList)
        {
            foreach(var w in d.words)
            {
                if (!tempDic.ContainsKey(w))
                {
                    tempDic[w] = new HashSet<Document>();
                }
                tempDic[w].Add(d);
            }
        }
        var dictionary = tempDic.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToList());

        return dictionary;
    }
}