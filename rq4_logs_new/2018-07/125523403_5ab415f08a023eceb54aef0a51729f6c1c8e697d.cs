using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Common.Log;
using Lykke.Common.ExchangeAdapter.Contracts;
using Lykke.Common.ExchangeAdapter.Tools.ObservableWebSocket;
using Lykke.Service.BitfinexAdapter.Core.Domain.WebSocketClient;
using Newtonsoft.Json.Linq;

namespace Lykke.Service.BitfinexAdapter.Services.OrderBooks
{
    public sealed class BitfinexOrderBookWebSocketReader
    {
        public BitfinexOrderBookWebSocketReader(ILog log)
        {
            _log = log;
        }

        private readonly ConcurrentDictionary<long, (string, OrderBook)> _subscriptions
            = new ConcurrentDictionary<long, (string, OrderBook)>();

        private readonly ILog _log;

        public OrderBook DeserializeMessage(ISocketEvent msg)
        {
            if (msg is IMessageReceived<byte[]> asBytes)
            {
                var json = JToken.Parse(Encoding.UTF8.GetString(asBytes.Content));

                if (json is JArray arr)
                {
                    return ProcessChannelUpdate(arr);
                }
                else
                {
                    ProcessResponse(json);

                    return null;
                }
            }

            return null;
        }

        private OrderBook ProcessChannelUpdate(JArray arr)
        {
            if (arr.Count < 2)
            {
                return null;
            }

            var chanId = arr[0].Value<long>();

            if (arr.Count == 2 &&
                arr[1].Type == JTokenType.String &&
                arr[1].Value<string>() == "hb")
            {
                return null;
            }

            if (arr[1] is JArray snapshot)
            {
                return ProcessSnapshot(chanId, snapshot.Cast<JArray>());
            }
            else
            {
                return ProcessUpdate(chanId, arr);
            }
        }

        private const string SOURCE = "bitfinex";

        private OrderBook ProcessUpdate(long chanId, JArray arr)
        {
            var price = arr[1].Value<decimal>();
            var count = arr[2].Value<int>();

            return _subscriptions.AddOrUpdate(chanId,
                _ => throw new InvalidOperationException("Received channel update before subscription"),
                (_, t) =>
                {
                    var orderBook = t.Item2.Clone(DateTime.UtcNow);

                    if (orderBook == null)
                        throw new InvalidOperationException("Received channel update before snapshot");

                    if (count == 0)
                    {
                        orderBook.UpdateAsk(price, 0);
                        orderBook.UpdateBid(price, 0);
                    }
                    else
                    {
                        var amount = arr[3].Value<decimal>();

                        if (amount < 0) orderBook.UpdateAsk(price, Math.Abs(amount));
                        if (amount > 0) orderBook.UpdateBid(price, Math.Abs(amount));
                    }

                    return (t.Item1, orderBook);
                }).Item2;
        }

        private OrderBook ProcessSnapshot(long chanId, IEnumerable<JArray> snapshot)
        {
            var orders = snapshot
                .Select(x => (x[0].Value<decimal>(), x[2].Value<decimal>()))
                .ToArray();

            return _subscriptions.AddOrUpdate(chanId,
                _ => throw new InvalidOperationException("Received channel snapshot before subscription"),
                (_, t) =>
                {
                    return (t.Item1, new OrderBook(SOURCE, t.Item1, DateTime.UtcNow,
                        asks: orders.Where(x => x.Item2 < 0).Select(
                            x => new OrderBookItem(x.Item1, Math.Abs(x.Item2))),
                        bids: orders.Where(x => x.Item2 > 0).Select(
                            x => new OrderBookItem(x.Item1, Math.Abs(x.Item2)))));
                }
            ).Item2;
        }

        private void ProcessResponse(JToken json)
        {
            var ev = EventResponse.Parse(json);

            if (ev is SubscribedResponse s)
            {
                _subscriptions[s.ChanId] = (s.Pair, null);
            }
        }
    }
}