public class DocHandler
{
    public FileReader fr{get; set;}
    public FileEditor fe{get; set;}

    public DocHandler(FileReader fr, FileEditor fe)
    {
        this.fr = fr;
        this.fe = fe;
    }

    public async Task<Document> ExtractDoc(string dockPath)
    {
        var str = await fr.Read("./EnglishData/"+Path.GetFileName(dockPath));
        var myString = fe.RemoveExtraSpace(str);
        var words = fe.Split(fe.ToUpper(myString));
        Document doc = new Document(Path.GetFileName(dockPath), "./EnglishData/"+Path.GetFileName(dockPath), myString, words);
        return doc;
    } 
    
}