namespace MarketVault.Core.Services.Impementations
{
    using MarketVault.Core.Contracts;
    using MarketVault.Core.Services.Interfaces;
    using MarketVault.Infrastructure.Data.Models;
    using Microsoft.Extensions.Logging;
    using System.Threading.Tasks;

    /// <summary>
    /// Barcode service
    /// </summary>
    public class BarcodeService : IBarcodeService
    {
        /// <summary>
        /// Barcode repository
        /// </summary>
        private readonly IRepository<Barcode> repository = null!;

        /// <summary>
        /// Logger
        /// </summary>
        private readonly ILogger<BarcodeService> logger = null!;

        /// <summary>
        /// Default constructor, injection of Barcode repository and logger (DI)
        /// </summary>
        /// <param name="repository">Barcode repository</param>
        /// <param name="logger">Logger</param>
        public BarcodeService(IRepository<Barcode> repository,
            ILogger<BarcodeService> logger)
        {
            this.repository = repository;
            this.logger = logger;
        }

        /// <summary>
        /// Add barcode method (Asynchronous)
        /// </summary>
        /// <param name="barcode">Barcode to add</param>
        /// <returns>(void)</returns>
        public async Task AddAsync(Barcode barcode)
        {
            logger.LogInformation("Add async method invoked in barcode service.");

            await this.repository.AddAsync(barcode);
        }
    }
}