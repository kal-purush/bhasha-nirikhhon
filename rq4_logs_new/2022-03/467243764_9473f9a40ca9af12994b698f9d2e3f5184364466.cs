using System;
using System.Collections.Generic;
using System.Linq;
using Tribal.Backend.CreditLine.Domain;
using Tribal.Backend.CreditLine.Domain.Models;
using Tribal.Backend.CreditLine.Infrastructure.DataRepositories;

namespace Tribal.Backend.CreditLine.Application
{
    public class UserLogService
    {
        private const int TWO_MINUTES = 120000;
        private const int THIRTY_SECONDS = 30000;
        UserLogRepository _userLogRepository;

        public UserLogService(UserLogRepository userLogRepository)
        {
            _userLogRepository = userLogRepository ?? throw new ArgumentNullException(nameof(userLogRepository), "Argument can not be null");
        }

        public bool CheckAttemptCounter(string credential, DateTime requestTime, bool lastApplicationWasAccepted)
        {
            bool response;
            List<UserRequest> requestList;
            try
            {
                Guid customerId = Guid.Parse(credential);
                int timeValidator = lastApplicationWasAccepted ? TWO_MINUTES : THIRTY_SECONDS;
                int countValidator = lastApplicationWasAccepted ? 3 : 1;

                requestList = _userLogRepository.Find(log => log.CustomerId == customerId
                                                         && log.RequestType == "DETERMINECREDITLINE"
                                                         && log.CreatedAt <= requestTime.AddMilliseconds(timeValidator))
                                               .ToList();
                response = requestList.Count >= countValidator;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return response;
        }

        public void SaveRequest(CreditLineRequestModel creditLineRequest, string credential)
        {
            UserRequest item;

            try
            {
                Guid customerId = Guid.Parse(credential);
                item = new UserRequest()
                {
                    Guid = Guid.NewGuid(),
                    CreatedAt = creditLineRequest.RequestedDate,
                    CustomerId = customerId,
                    Request = creditLineRequest,
                    RequestType = "DETERMINECREDITLINE"
                };
                _userLogRepository.Insert(item);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}