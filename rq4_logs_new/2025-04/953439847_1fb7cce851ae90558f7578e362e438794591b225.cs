using BuildingBlocks.Application.Ports;
using Domain;
using MediatR;

namespace Application.EmbeddingsComputing
{
    public class ComputeEmbeddginsCommandHandler : IRequestHandler<ComputeEmbeddingsCommand, int>
    {
        private readonly IStorage _storage;
        private readonly IPdfRecognizer _pdfRecognizer;
        private readonly ITextEmbeddingsProvider _textEmbeddingsProvider;
        public ComputeEmbeddginsCommandHandler(IStorage storage, IPdfRecognizer pdfRecognizer, ITextEmbeddingsProvider textEmbeddingsProvider)
        {
            _storage = storage;
            _pdfRecognizer = pdfRecognizer;
            _textEmbeddingsProvider = textEmbeddingsProvider;
        }
        public async Task<int> Handle(ComputeEmbeddingsCommand request, CancellationToken cancellationToken)
        {
            var embeddings = new List<Embedding>();
            var files = await _storage.GetFilesNames("documents", "pdf");
            
            foreach (var file in files)
            {
                var storageFile = await _storage.GetFile("documents", file);
                var recognizedDocument = await _pdfRecognizer.Recognize(storageFile.Content);
                int pageNo = 0;

                foreach (var page in recognizedDocument.Pages)
                {
                    var computedPageEmbedding = await _textEmbeddingsProvider.GetEmbeddings(page);
                    embeddings.Add(new Embedding { FileName = file, Page =  pageNo, Vectors = computedPageEmbedding  });
                    pageNo++;
                }

                // store in azure ai search
                // await _azureAiSearchClient.IndexDocumentsAsync(embeddings);
            }
            return files.Count();
        }
    }
    
}