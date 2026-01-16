using FluentValidation;
using ItemsReview.Interfaces;
using ItemsReview.Models;
using Mapster;
using Shared.Core;
using Shared.Core.Models;
using Shared.Core.Utilities;
using System;
using System.Collections.Generic;
using System.Net;

namespace ItemsReview.Services
{
    public class ItemsReviewsManager : IItemsReviewsManager
    {
        protected IUnitOfWork _unitOfWork;
        private readonly IValidator<ItemReviewDto> _validator;
        public ItemsReviewsManager(IUnitOfWork unitOfWork, IValidator<ItemReviewDto> validator)
        {
            _unitOfWork = unitOfWork;
            _validator = validator;
        }
        public ResultMessage GetAll()
        {
            try
            {
                IEnumerable<ItemReviewDto> result = new List<ItemReviewDto>();
                result = _unitOfWork.ItemsReviewsRepository.Get().Adapt(result);

                return new ResultMessage()
                {
                    Data = result,
                    Status = HttpStatusCode.OK
                };
            }
            catch (Exception ex)
            {
                //log ex
                return new ResultMessage()
                {
                    ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsGetAllError,
                    Status = HttpStatusCode.InternalServerError
                };
            }
        }
        public ResultMessage Insert(ItemReviewDto newItemDto)
        {
            var validationResult = _validator.Validate(newItemDto);
            if (!validationResult.IsValid)
                return new ResultMessage
                {
                    Status = HttpStatusCode.BadRequest,
                    ValidationMessages = validationResult.GetErrorsList()
                };

            try
            {
                _unitOfWork.ItemsReviewsRepository.Insert(newItemDto.Adapt<ItemsForReview>());
                _unitOfWork.Commit();
                return new ResultMessage()
                {
                    Status = HttpStatusCode.OK
                };
            }
            catch (Exception ex)
            {
                return new ResultMessage()
                {
                    ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsInsertError,
                    Status = HttpStatusCode.InternalServerError
                };
            }
        }
        public ResultMessage GetById(int id)
        {
            try
            {
                var itemReview = _unitOfWork.ItemsReviewsRepository.GetById(id);
                if (itemReview != null)
                    return new ResultMessage()
                    {
                        Data = itemReview.Adapt<ItemReviewDto>(),
                        Status = HttpStatusCode.OK
                    };
                else
                    return new ResultMessage()
                    {
                        Status = HttpStatusCode.NotFound,
                        ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsNotFoundError
                    };
            }
            catch (Exception ex)
            {
                //log ex
                return new ResultMessage()
                {
                    ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsGetByIdError,
                    Status = HttpStatusCode.InternalServerError
                };
            }
        }
        public ResultMessage Update(ItemReviewDto itemReviewDto, int id)
        {
            var validationResult = _validator.Validate(itemReviewDto);
            if (!validationResult.IsValid)
                return new ResultMessage
                {
                    Status = HttpStatusCode.BadRequest,
                    ValidationMessages = validationResult.GetErrorsList()
                };

            try
            {
                var oldItem = _unitOfWork.ItemsReviewsRepository.GetById(id);
                if (oldItem != null)
                {
                    oldItem.Name = itemReviewDto.Name;
                    oldItem.ProfilePicture = itemReviewDto.ProfilePicture;
                    _unitOfWork.ItemsReviewsRepository.Update(oldItem);
                    _unitOfWork.Commit();
                    return new ResultMessage
                    {
                        Status = HttpStatusCode.OK
                    };
                }
                else
                {
                    return new ResultMessage
                    {
                        Status = HttpStatusCode.NotFound,
                        ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsNotFoundError
                    };
                }
            }
            catch (Exception ex)
            {
                return new ResultMessage
                {
                    Status = HttpStatusCode.InternalServerError,
                    ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsUpdateError
                };
            }
        }
        public ResultMessage Delete(int id)
        {
            try
            {
                _unitOfWork.ItemsReviewsRepository.Delete(id);
                _unitOfWork.Commit();
                return new ResultMessage()
                {
                    Status = HttpStatusCode.OK
                };
            }
            catch (Exception ex)
            {
                return new ResultMessage
                {
                    Status = HttpStatusCode.InternalServerError,
                    ErrorCode = (int)ItemsReviewsErrorsCodeEnum.ItemsDeleteError
                };
            }
        }
    }
}