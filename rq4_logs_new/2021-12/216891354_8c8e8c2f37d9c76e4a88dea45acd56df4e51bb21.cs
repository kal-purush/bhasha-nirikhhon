namespace EstateManagement.BusinessLogic.Requests
{
    using System;
    using MediatR;

    public class GenerateMerchantStatementRequest : IRequest<Guid>
    {
        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="GenerateMerchantStatementRequest" /> class.
        /// </summary>
        /// <param name="estateId">The estate identifier.</param>
        /// <param name="merchantId">The merchant identifier.</param>
        /// <param name="statementDate">The statement date.</param>
        private GenerateMerchantStatementRequest(Guid estateId,
                                                 Guid merchantId,
                                                 DateTime statementDate)
        {
            this.EstateId = estateId;
            this.MerchantId = merchantId;
            this.StatementDate = statementDate;
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the estate identifier.
        /// </summary>
        /// <value>
        /// The estate identifier.
        /// </value>
        public Guid EstateId { get; }

        /// <summary>
        /// Gets the merchant identifier.
        /// </summary>
        /// <value>
        /// The merchant identifier.
        /// </value>
        public Guid MerchantId { get; }

        /// <summary>
        /// Gets the statement date time.
        /// </summary>
        /// <value>
        /// The statement date time.
        /// </value>
        public DateTime StatementDate { get; }

        #endregion

        #region Methods

        /// <summary>
        /// Creates the specified merchant statement identifier.
        /// </summary>
        /// <param name="estateId">The estate identifier.</param>
        /// <param name="merchantId">The merchant identifier.</param>
        /// <param name="statementDate">The statement date.</param>
        /// <returns></returns>
        public static GenerateMerchantStatementRequest Create(Guid estateId,
                                                              Guid merchantId,
                                                              DateTime statementDate)
        {
            return new GenerateMerchantStatementRequest(estateId, merchantId, statementDate);
        }

        #endregion
    }
}