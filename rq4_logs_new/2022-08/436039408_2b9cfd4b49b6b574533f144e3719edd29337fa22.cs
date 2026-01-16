using CarAuction.Data.Enums;
using CarAuction.Data.Interfaces;
using CarAuction.Logic.Commands.Auction;
using CarAuction.Logic.Models;
using MediatR;
using System.Threading;
using System.Threading.Tasks;

namespace CarAuction.Logic.Handlers
{
    public class ValidateAuctionBehavior :
        IPipelineBehavior<UpdateAuctionCommand, ResponseModel>
    {
        private readonly IAuctionRepository _repo;

        public ValidateAuctionBehavior(IAuctionRepository repository)
        {
            _repo = repository;
        }

        public async Task<ResponseModel> Handle(UpdateAuctionCommand request, CancellationToken cancellationToken, RequestHandlerDelegate<ResponseModel> next)
        {
            var auctionToUpdate = await _repo.GetById(request.Id);
            if (auctionToUpdate == null)
            {
                return null;
            }
            if (auctionToUpdate.Status != AuctionStatus.Planned)
            {
                return new ResponseModel
                {
                    Result = false,
                    Message = $"You cannot update the auction, which has started or completed"
                };
            }

            return await next();
        }
    }
}