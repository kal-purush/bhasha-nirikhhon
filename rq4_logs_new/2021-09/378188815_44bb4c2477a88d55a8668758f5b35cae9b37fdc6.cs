using leave_management.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace leave_management.Contracts
{
   public interface IEmailRepository
    {
        void SendEmail(EmailMessage message);
    }
}