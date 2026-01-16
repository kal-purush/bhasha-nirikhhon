using BuildingBlocks.Application.Ports;
using MediatR;

namespace Application.Completions
{
    class TextCompletionQueryHandler : IRequestHandler<TextCompletionQuery, string>
    {
        private readonly IChatCompletionProvider _chatCompletionProvider;

        public TextCompletionQueryHandler(IChatCompletionProvider chatCompletionProvider)
        {
            _chatCompletionProvider = chatCompletionProvider;
        }

        public async Task<string> Handle(TextCompletionQuery request, CancellationToken cancellationToken)
        {            
            return await _chatCompletionProvider.CompleteChatAsync(request.Prompt);
        }
    }    
}