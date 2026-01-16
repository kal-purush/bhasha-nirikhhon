using System.Collections.Generic;
using System.Threading.Tasks;
using Core.WebDtos;
using Core.Entities.Halko;
using Core.Interfaces;
using Core.Specifications;

namespace Infrastructure.Services
{
    public class TransactionService : ITransactionService
    {
        private readonly IUnitOfWork _unitOfWork;

        public TransactionService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }
        
        public async Task<Transaction> CreateTransactionAsync( TransactionToInsertDto transactionDto )
        {
            if( string.IsNullOrEmpty ( transactionDto.ProductName ) || transactionDto.Price <= 0 ) return null;
            
            
            var pointSpec = new PointSpecification ( transactionDto.PointName );
            var point = await _unitOfWork.Repository<Point>().GetEntityWithSpecAsync ( pointSpec );
            
            
            var participantSpec = new ParticipantSpecification ( transactionDto.ParticipantInitial, point.Id );
            var participant = await _unitOfWork.Repository<ParticipantPoint>().GetEntityWithSpecAsync ( participantSpec );

            
            var productCategorySpec = new ProductCategoriesSpecification ( transactionDto.ProductCategoryName, transactionDto.TransactionType );
            var productCategory = await _unitOfWork.Repository<ProductCategory>().GetEntityWithSpecAsync ( productCategorySpec );

            
            if( participant == null || productCategory == null ) return null;

            
            var transactionToSave = new Transaction
            {
                ProductName = transactionDto.ProductName,
                Price = transactionDto.Price,
                Participant = participant,
                ProductCategory = productCategory,
                TransactionType = productCategory.TransactionType,
                Point = point
            };

            
            _unitOfWork.Repository<Transaction>().Add ( transactionToSave );
            var result = await _unitOfWork.CompleteAsync();

            
            return result <= 0 ? null : transactionToSave;
        }


        public async Task<IReadOnlyList<ProductCategory>> GetProductCategories()
        {
            var productCategories = await _unitOfWork.Repository<ProductCategory>()
                .ListAsync ( new ProductCategoriesSpecification() );

            return productCategories;
        }
    }
}