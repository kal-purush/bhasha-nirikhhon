using SFA.DAS.ApprenticeCommitments.Data.Models;
using SFA.DAS.ApprenticeCommitments.Infrastructure.Mediator;
using System;

#nullable enable

namespace SFA.DAS.ApprenticeCommitments.Application.Commands.ConfirmCommand
{
    public class ConfirmCommand : IUnitOfWorkCommand
    {
        public ConfirmCommand(
            Guid apprenticeId, long apprenticeshipId, long commitmentStatementId,
            Confirmations confirmations)
        {
            ApprenticeId = apprenticeId;
            ApprenticeshipId = apprenticeshipId;
            CommitmentStatementId = commitmentStatementId;
            Confirmations = confirmations;
        }

        public Guid ApprenticeId { get; }
        public long ApprenticeshipId { get; }
        public long CommitmentStatementId { get; }
        public Confirmations Confirmations { get; }
    }
}