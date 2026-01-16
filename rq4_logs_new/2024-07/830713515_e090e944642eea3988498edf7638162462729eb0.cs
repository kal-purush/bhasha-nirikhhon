public class DocumentExtractor : IDocumentExtractor
{
    private static DocumentExtractor instance;

    private DocumentExtractor(){}

    public static DocumentExtractor getInstance() {
        if(instance == null)
            instance = new DocumentExtractor();
        return instance;
    }

    public async Task<List<Document>> ExtractDocuments(List<string> filePaths, TextProcessor fileProcessor)
    {
        List<Document> documents = new List<Document>();
            var multy = MultiTextFileReader.getInstance();
            var texts = await multy.MultiReadAsync(filePaths);
            for(int i=0; i<filePaths.Count; i++) {
                var words = fileProcessor.Split(fileProcessor.ToUpper(fileProcessor.RemoveExtraSpace(texts[i])));
                Document d =new Document(Path.GetFileName(filePaths[i]), filePaths[i], texts[i], words);
                documents.Add(d);
            }
            return documents;
    }

}