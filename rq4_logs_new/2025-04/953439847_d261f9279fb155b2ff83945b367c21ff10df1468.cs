using BuildingBlocks.Application.Ports;
using MediatR;

namespace Application.Embeddings
{
    public class TextEmbeddingQueryHandler : IRequest<TextEmbeddingQuery>
    {
        private readonly ITextEmbeddingsProvider _textEmbeddingsProvider;

        public TextEmbeddingQueryHandler(ITextEmbeddingsProvider textEmbeddingsProvider)
        {
            _textEmbeddingsProvider = textEmbeddingsProvider;
        }
        public async Task<IReadOnlyList<float>> Handle(TextEmbeddingQuery request, CancellationToken cancellationToken)
        {
            return await _textEmbeddingsProvider.GetEmbeddings(request.Text);
        }
    }
    
}