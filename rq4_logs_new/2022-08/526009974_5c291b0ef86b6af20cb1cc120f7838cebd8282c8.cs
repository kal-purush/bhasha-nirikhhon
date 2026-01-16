using MediatR;
using Noteflix.Application.Responses;

namespace Noteflix.Application.Commands
{
    public class CreateNotebookCommand : IRequest<CreateNotebookCommandResponse>
    {
        public string Title { get; set; }
    }
}