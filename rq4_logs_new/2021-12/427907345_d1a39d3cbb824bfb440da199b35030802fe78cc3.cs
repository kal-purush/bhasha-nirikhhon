using System.Collections.Generic;
using System.Threading.Tasks;
using EbSoft.Warehouse.SDK.Extensions;
using Warehouse.Core;
using WebRequest.Elegant;

namespace EbSoft.Warehouse.SDK
{
    public class EbSoftReceptionGoods : IEntities<IReceptionGood>
    {
        private readonly IWebRequest _server;
        private readonly string _receptionId;
        private readonly IFilter _filter;

        public EbSoftReceptionGoods(
            IWebRequest server,
            string receptionId
        ) : this(
                server,
                receptionId,
                new ReceptionsGoodsFilter(receptionId)
            )
        {
        }

        public EbSoftReceptionGoods(
            IWebRequest server,
            string receptionId,
            IFilter filter)
        {
            _server = server;
            _receptionId = receptionId;
            _filter = filter;
        }

        public Task<IList<IReceptionGood>> ToListAsync()
        {
            return _server
                .WithFilter(_filter)
                .SelectAsync<IReceptionGood>((good) => new EbSoftReceptionGood(_receptionId, good));
        }

        public IEntities<IReceptionGood> With(IFilter filter)
        {
            return new EbSoftReceptionGoods(
                _server,
                _receptionId,
                filter
            );
        }
    }
}