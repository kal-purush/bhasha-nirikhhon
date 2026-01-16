using CartService.Application.Specifications;
using CartService.Application.Specifications.Jobs;
using Hangfire;

namespace CartService.Infrastructure.Implementations.Jobs
{
    public class CartJobService: ICartJobService
    {
        private readonly IUnitOfWork _unitOfWork;

        public CartJobService(IUnitOfWork unitOfWork)
        {
            _unitOfWork = unitOfWork;
        }

        public async Task ScheduleJobAsync(string userId, CancellationToken cancellationToken)
        {
            var newJobId = BackgroundJob.Schedule(
               () => ExecuteJobAsync(userId),
               TimeSpan.FromHours(3));

            await _unitOfWork.CartJobRepository.SaveJobIdAsync(userId, newJobId, cancellationToken);
        }

        public async Task ExecuteJobAsync(string userId)
        {
            var cart = await _unitOfWork.CartRepository.GetCartAsync(userId, CancellationToken.None);

            if (cart is null)
            {
                return;
            }

            await _unitOfWork.CartRepository.DeleteCartAsync(userId, CancellationToken.None);

            await DeleteJobAsync(userId, CancellationToken.None);
        }

        public async Task DeleteJobAsync(string userId, CancellationToken cancellationToken)
        {
            var jobId = await _unitOfWork.CartJobRepository.GetJobIdAsync(userId, cancellationToken);

            if (!string.IsNullOrEmpty(jobId))
            {
                BackgroundJob.Delete(jobId);

                await _unitOfWork.CartJobRepository.DeleteJobIdAsync(userId, cancellationToken);
            }
        }
    }
}