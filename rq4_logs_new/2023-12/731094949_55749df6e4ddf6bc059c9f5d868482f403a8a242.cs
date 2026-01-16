using AutoMapper;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore;
using QRisto.Application.Errors;
using QRisto.Application.Models.Request.Comment;
using QRisto.Application.Models.Response.Comment;
using QRisto.Application.Services.User;
using QRisto.Application.Utils;
using QRisto.Persistence.Entity.Auth;
using QRisto.Persistence.Entity.Provider;
using QRisto.Persistence.Repositories.Implementations;

namespace QRisto.Application.Services.Comment;

public class CommentService : ICommentService
{
    private readonly UnitOfWork _unitOfWork;
    private readonly UserManager<ApplicationUser> _userManager;
    private readonly IUserService _userService;
    private readonly IMapper _mapper;
    
    public CommentService(
        UnitOfWork unitOfWork,
        UserManager<ApplicationUser> userManager,
        IMapper mapper,
        IUserService userService)
    {
        _unitOfWork = unitOfWork;
        _userManager = userManager;
        _mapper = mapper;
        _userService = userService;
    }

    public async Task<Result<CommentResponseModel>> CreateAsync(CreateCommentRequestModel model)
    {
        try
        {
            var user = await _userManager.Users.FirstOrDefaultAsync(x => x.Id == model.UserId);

            if (user == null)
            {
                return Result<CommentResponseModel>.Failure(UserErrors.NotFound);
            }

            var service = await _unitOfWork.ServiceRepository.GetByIdAsync(model.ServiceId);

            if (service == null)
            {
                return Result<CommentResponseModel>.Failure(ServiceErrors.NotFound);
            }

            var comment = _mapper.Map<CommentEntity>(model);

            var createdComment = await _unitOfWork.CommentRepository.InsertAsync(comment);

            await _unitOfWork.SaveChangesAsync();

            var result = _mapper.Map<CommentResponseModel>(createdComment);
            
            return Result<CommentResponseModel>.Success(result);
        }
        catch (Exception ex)
        {
            return Result<CommentResponseModel>.Failure(ex.ToString());
        }
    }

    public async Task<Result<CommentResponseModel>> UpdateAsync(UpdateCommentRequestModel model)
        {
            try
            {
                var comment = await _unitOfWork.CommentRepository.GetByIdAsync(model.CommentId);

                if (comment == null)
                {
                    return Result<CommentResponseModel>.Failure("Comment not found.");
                }
                
                var user = await _userManager.Users.FirstOrDefaultAsync(x => x.Id == model.UserId);

                if (user == null)
                {
                    return Result<CommentResponseModel>.Failure(UserErrors.NotFound);
                }

                var service = await _unitOfWork.ServiceRepository.GetByIdAsync(model.ServiceId);

                if (service == null)
                {
                    return Result<CommentResponseModel>.Failure(ServiceErrors.NotFound);
                }

                _mapper.Map(model, comment);

                _unitOfWork.CommentRepository.Update(comment);
                await _unitOfWork.SaveChangesAsync();

                var result = _mapper.Map<CommentResponseModel>(comment);

                return Result<CommentResponseModel>.Success(result);
            }
            catch (Exception ex)
            {
                return Result<CommentResponseModel>.Failure(ex.ToString());
            }
        }

        public async Task<Result> DeleteAsync(Guid commentId)
        {
            try
            {
                var comment = await _unitOfWork.CommentRepository.GetByIdAsync(commentId);

                if (comment == null)
                {
                    return Result.Failure("Comment not found.");
                }

                var currentUserIdResult = await _userService.GetCurrentAuthorizedUserIdAsync();

                if (!currentUserIdResult.IsSuccess)
                {
                    return Result.Failure(currentUserIdResult.Error);
                }
                
                await _unitOfWork.CommentRepository.DeleteAsync(commentId, currentUserIdResult.Value);
                await _unitOfWork.SaveChangesAsync();

                return Result.Success();
            }
            catch (Exception ex)
            {
                return Result.Failure(ex.ToString());
            }
        }

        public async Task<Result<List<CommentResponseModel>>> GetServiceCommentsAsync(Guid serviceId, int page, int pageSize)
        {
            try
            {
                var service = await _unitOfWork.ServiceRepository.GetByIdAsync(serviceId);

                if (service == null)
                {
                    return Result<List<CommentResponseModel>>.Failure(ServiceErrors.NotFound);
                }
                
                var comments = await _unitOfWork.CommentRepository
                    .GetServiceCommentsWithPaginationAsync(serviceId, page, pageSize);

                var result = _mapper.Map<List<CommentResponseModel>>(comments);

                return Result<List<CommentResponseModel>>.Success(result);
            }
            catch (Exception ex)
            {
                return Result<List<CommentResponseModel>>.Failure(ex.ToString());
            }
        }

        public async Task<Result<List<CommentResponseModel>>> GetUserCommentsAsync(Guid userId, int page, int pageSize)
        {
            try
            {
                var user = await _userManager.Users.FirstOrDefaultAsync(x => x.Id == userId);

                if (user == null)
                {
                    return Result<List<CommentResponseModel>>.Failure(UserErrors.NotFound);
                }
                
                var comments = await _unitOfWork.CommentRepository
                    .GetUserCommentsWithPaginationAsync(userId, page, pageSize);

                var result = _mapper.Map<List<CommentResponseModel>>(comments);

                return Result<List<CommentResponseModel>>.Success(result);
            }
            catch (Exception ex)
            {
                return Result<List<CommentResponseModel>>.Failure(ex.ToString());
            }
        }

        public async Task<Result<CommentResponseModel>> GetCommentAsync(Guid commentId)
        {
            try
            {
                var comments = await _unitOfWork.CommentRepository
                    .GetByIdAsync(commentId);

                var result = _mapper.Map<CommentResponseModel>(comments);

                return Result<CommentResponseModel>.Success(result);
            }
            catch (Exception ex)
            {
                return Result<CommentResponseModel>.Failure(ex.ToString());
            }
        }
}