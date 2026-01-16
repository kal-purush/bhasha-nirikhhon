using LT.DigitalOffice.Broker.Requests;
using LT.DigitalOffice.Broker.Responses;
using LT.DigitalOffice.Kernel.AccessValidatorEngine.Interfaces;
using LT.DigitalOffice.Kernel.Broker;
using LT.DigitalOffice.Kernel.Constants;
using LT.DigitalOffice.Kernel.Exceptions.Models;
using LT.DigitalOffice.Kernel.Extensions;
using LT.DigitalOffice.UserService.Business.Commands.Certificate.Interfaces;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Mappers.Db.Interfaces;
using LT.DigitalOffice.UserService.Models.Dto.Enums;
using LT.DigitalOffice.UserService.Models.Dto.Requests.User;
using LT.DigitalOffice.UserService.Models.Dto.Requests.User.Certificates;
using LT.DigitalOffice.UserService.Models.Dto.Responses;
using MassTransit;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace LT.DigitalOffice.UserService.Business.Commands.Certificate
{
    public class CreateCertificateCommand : ICreateCertificateCommand
    {
        private IAccessValidator _accessValidator;
        private IHttpContextAccessor _httpContextAccessor;
        private IUserRepository _userRepository;
        private ICertificateRepository _certificateRepository;
        private IDbUserCertificateMapper _mapper;
        private readonly IRequestClient<IAddImageRequest> _rcImage;
        private readonly ILogger<CreateCertificateCommand> _logger;

        private Guid? GetImageId(AddImageRequest addImageRequest, List<string> errors)
        {
            Guid? imageId = null;

            if (addImageRequest == null)
            {
                throw new ArgumentNullException(nameof(addImageRequest));
            }

            Guid userId = _httpContextAccessor.HttpContext.GetUserId();

            string errorMessage = "Can not add certificate image to certificate. Please try again later.";

            try
            {
                var response = _rcImage.GetResponse<IOperationResult<IAddImageResponse>>(
                    IAddImageRequest.CreateObj(
                        addImageRequest.Name,
                        addImageRequest.Content,
                        addImageRequest.Extension,
                        userId), default, TimeSpan.FromSeconds(5)).Result;

                if (!response.Message.IsSuccess)
                {
                    _logger.LogWarning(
                        errorMessage + $"Reason: '{string.Join(',', response.Message.Errors)}'");

                    errors.Add(errorMessage);
                }
                else
                {
                    imageId = response.Message.Body.Id;
                }
            }
            catch (Exception exc)
            {
                _logger.LogError(exc, errorMessage);

                errors.Add(errorMessage);
            }

            return imageId;
        }

        public CreateCertificateCommand(
            IAccessValidator accessValidator,
            IHttpContextAccessor httpContextAccessor,
            IDbUserCertificateMapper mapper,
            IUserRepository userRepository,
            ICertificateRepository certificateRepository,
            IRequestClient<IAddImageRequest> rcAddIImage,
            ILogger<CreateCertificateCommand> logger)
        {
            _accessValidator = accessValidator;
            _httpContextAccessor = httpContextAccessor;
            _mapper = mapper;
            _userRepository = userRepository;
            _certificateRepository = certificateRepository;
            _rcImage = rcAddIImage;
            _logger = logger;
        }

        public OperationResultResponse<Guid> Execute(CreateCertificateRequest request)
        {
            List<string> errors = new();

            var senderId = _httpContextAccessor.HttpContext.GetUserId();
            var dbUser = _userRepository.Get(senderId);
            if (!(dbUser.IsAdmin ||
                  _accessValidator.HasRights(Rights.AddEditRemoveUsers))
                  && senderId != request.UserId)
            {
                throw new ForbiddenException("Not enough rights.");
            }

            Guid? imageId = GetImageId(request.Image, errors);

            if (!imageId.HasValue)
            {
                return new OperationResultResponse<Guid>
                {
                    Status = OperationResultStatusType.Failed,
                    Errors = errors
                };
            }

            var dbUserCertificate = _mapper.Map(request, imageId.Value);

            _certificateRepository.Add(dbUserCertificate);

            return new OperationResultResponse<Guid>
            {
                Status = OperationResultStatusType.FullSuccess,
                Body = dbUserCertificate.Id
            };
        }
    }
}