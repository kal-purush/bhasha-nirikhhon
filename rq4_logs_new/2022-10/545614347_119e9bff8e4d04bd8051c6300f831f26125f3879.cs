using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SOLID
{
    internal class DIP
    {
    }

    public interface IRemoteService
    {
        public void PressOnButton();
    }

    public interface ITvService
    {
        public void TurnOn();
    }

    public class RemoteService : IRemoteService
    {
        private readonly ITvService _tvService;

        public RemoteService(ITvService tvService)
        {
            _tvService = tvService;
        }
        
        public void PressOnButton()
        {
            _tvService.TurnOn();
        }
    }
}