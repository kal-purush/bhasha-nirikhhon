namespace EstateManagement.MerchantStatement.DomainEvents
{
    using System;
    using Shared.DomainDrivenDesign.EventSourcing;

    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Shared.DomainDrivenDesign.EventSourcing.DomainEventRecord.DomainEvent" />
    /// <seealso cref="Shared.DomainDrivenDesign.EventSourcing.IDomainEvent" />
    /// <seealso cref="System.IEquatable&lt;Shared.DomainDrivenDesign.EventSourcing.DomainEventRecord.DomainEvent&gt;" />
    /// <seealso cref="System.IEquatable&lt;EstateManagement.MerchantStatement.DomainEvents.SettledFeeAddedToStatementEvent&gt;" />
    public record SettledFeeAddedToStatementEvent : DomainEventRecord.DomainEvent
    {
        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="SettledFeeAddedToStatementEvent"/> class.
        /// </summary>
        /// <param name="aggregateId">The aggregate identifier.</param>
        /// <param name="estateId">The estate identifier.</param>
        /// <param name="merchantId">The merchant identifier.</param>
        /// <param name="settledFeeId">The settled fee identifier.</param>
        /// <param name="transactionId">The transaction identifier.</param>
        /// <param name="settledDateTime">The settled date time.</param>
        /// <param name="settledValue">The settled value.</param>
        public SettledFeeAddedToStatementEvent(Guid aggregateId,
                                               Guid estateId,
                                               Guid merchantId,
                                               Guid settledFeeId,
                                               Guid transactionId,
                                               DateTime settledDateTime,
                                               Decimal settledValue) : base(aggregateId, Guid.NewGuid())
        {
            this.EstateId = estateId;
            this.MerchantId = merchantId;
            this.TransactionId = transactionId;
            this.SettledDateTime = settledDateTime;
            this.SettledValue = settledValue;
            this.SettledFeeId = settledFeeId;
            this.MerchantStatementId = aggregateId;
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the estate identifier.
        /// </summary>
        /// <value>
        /// The estate identifier.
        /// </value>
        public Guid EstateId { get; init; }

        /// <summary>
        /// Gets the merchant identifier.
        /// </summary>
        /// <value>
        /// The merchant identifier.
        /// </value>
        public Guid MerchantId { get; init; }

        /// <summary>
        /// Gets or sets the merchant statement identifier.
        /// </summary>
        /// <value>
        /// The merchant statement identifier.
        /// </value>
        public Guid MerchantStatementId { get; init; }

        /// <summary>
        /// Gets or sets the settled date time.
        /// </summary>
        /// <value>
        /// The settled date time.
        /// </value>
        public DateTime SettledDateTime { get; set; }

        /// <summary>
        /// Gets or sets the settled fee identifier.
        /// </summary>
        /// <value>
        /// The settled fee identifier.
        /// </value>
        public Guid SettledFeeId { get; set; }

        /// <summary>
        /// Gets or sets the settled value.
        /// </summary>
        /// <value>
        /// The settled value.
        /// </value>
        public Decimal SettledValue { get; set; }

        /// <summary>
        /// Gets or sets the transaction identifier.
        /// </summary>
        /// <value>
        /// The transaction identifier.
        /// </value>
        public Guid TransactionId { get; init; }

        #endregion
    }
}