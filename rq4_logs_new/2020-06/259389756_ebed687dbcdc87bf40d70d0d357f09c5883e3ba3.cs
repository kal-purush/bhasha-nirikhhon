using AutoMapper;
using Hippologamus.API.Manager.Interface;
using Hippologamus.API.Service.Service.Interface;
using Hippologamus.DTO.DTO;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Hippologamus.API.Manager
{
    public class PerfLogPerfItemManager : BaseManager, IPerfLogPerfItemManager
    {
        private IPerfLogPerfItemService _perfLogRequestPathService;

        public PerfLogPerfItemManager(IPerfLogPerfItemService perfLogRequestPathService, IMapper mapper)
            : base(mapper)
        {
            _perfLogRequestPathService = perfLogRequestPathService;
        }

        public async Task<List<PerfLogPerfItemCollection>> GetByAssembly(string perfLogAssembly)
        {
            return _mapper.Map<List<PerfLogPerfItemCollection>>(await _perfLogRequestPathService.GetByAssembly(perfLogAssembly));
        }
    }
}