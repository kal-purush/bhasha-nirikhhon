using Application.Interfaces.BlobStorageInterface;
using Application.Interfaces.OpenAiInterface;
using Domain.Models;
using MediatR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.Commands.CVCommands
{
    public class AnalyzeCvCommandHandler : IRequestHandler<AnalyzeCvCommand, AnalysisResult>
    {
        private readonly IBlobStorage _blobStorage;
        private readonly IOpenAiService _openAiService;

        public Task<AnalysisResult> Handle(AnalyzeCvCommand request, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}