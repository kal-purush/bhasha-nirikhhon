using MediatR;
using Microsoft.EntityFrameworkCore;
using RequestService.Application.Interfaces;
using RequestService.Infrastructure.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RequestService.Application.Commands.Answers.SetAnswerAsCorrectAnswer
{
    public class SetAnswerAsCorrectAnswerCommandHandler : INotificationHandler<SetAnswerAsCorrectAnswerCommand>
    {
        private readonly RequestServiceDbContext _context;
        private readonly INotificationService _notificationService;
        private readonly IMediator _mediator;

        public SetAnswerAsCorrectAnswerCommandHandler(RequestServiceDbContext context, INotificationService notificationService, IMediator mediator)
        {
            _context = context;
            _notificationService = notificationService;
            _mediator = mediator;
        }
        public async Task Handle(SetAnswerAsCorrectAnswerCommand notification, CancellationToken cancellationToken)
        {
            var entity = await _context.Requests.Include(a => a.Answers).FirstOrDefaultAsync(x => x.Id == notification.RequestId);

            var answerToEdit = entity.Answers.FirstOrDefault(a => a.Id == notification.AnswerId);

            answerToEdit.IsCorrectAnswer = true;

            await _context.SaveChangesAsync();
        }
    }
}