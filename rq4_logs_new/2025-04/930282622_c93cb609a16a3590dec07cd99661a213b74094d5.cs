using Bank.Application.Domain;
using Bank.Application.Queries;
using Bank.Application.Requests;
using Bank.ExchangeService.Database.Processors;
using Bank.ExchangeService.Models;
using Bank.ExchangeService.Repositories;
using Bank.Http.Clients.User;

namespace Bank.ExchangeService.BackgroundServices;

public class OrderBackgroundService(IRedisRepository redisRepository, IUserServiceHttpClient userServiceHttpClient, IOrderRepository orderRepository) : IRealtimeProcessor
{
    private readonly IRedisRepository       m_RedisRepository       = redisRepository;
    private readonly IOrderRepository       m_OrderRepository       = orderRepository;
    private readonly IUserServiceHttpClient m_UserServiceHttpClient = userServiceHttpClient;
    private          Guid                   m_SecurityTransactionCodeId;

    public async Task OnApplicationStarted()
    {
        var transactionCodeFilter = new TransactionCodeFilterQuery
                                    {
                                        Code = "280"
                                    };

        var transactionCodePageTask = m_UserServiceHttpClient.GetAllTransactionCodes(transactionCodeFilter, Pageable.Create(1, 1));

        await Task.WhenAll(transactionCodePageTask);

        var transactionCodePage = await transactionCodePageTask;

        m_SecurityTransactionCodeId = transactionCodePage.Items[0].Id;
    }

    public async Task ProcessStockQuotes(List<Quote> quotes)
    {
        var stockOrders = await m_RedisRepository.FindAllStockOrders();

        var orderQuoteList = quotes.GroupJoin(stockOrders, quote => quote.Security!.Ticker, order => order.Ticker, (quote, orders) => new { quote, orders })
                                   .SelectMany(group => group.orders.Select(order => new { group.quote, order }))
                                   .Where(pair => pair.order.OrderType is not OrderType.Market || pair.order.Direction is not Direction.Buy  || true)
                                   .Where(pair => pair.order.OrderType is not OrderType.Market || pair.order.Direction is not Direction.Sell || true)
                                   .Where(pair => pair.order.OrderType is not OrderType.Limit || pair.order.Direction is not Direction.Buy ||
                                                  pair.quote.BidPrice <= pair.order.LimitPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Limit || pair.order.Direction is not Direction.Sell ||
                                                  pair.quote.AskPrice >= pair.order.LimitPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Stop || pair.order.Direction is not Direction.Buy ||
                                                  pair.quote.BidPrice >= pair.order.StopPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Stop || pair.order.Direction is not Direction.Sell ||
                                                  pair.quote.AskPrice <= pair.order.StopPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.StopLimit || pair.order.Direction is not Direction.Buy ||
                                                  (pair.quote.BidPrice >= pair.order.StopPrice && pair.quote.AskPrice <= pair.order.LimitPrice))
                                   .Where(pair => pair.order.OrderType is not OrderType.StopLimit || pair.order.Direction is not Direction.Sell ||
                                                  (pair.quote.AskPrice <= pair.order.StopPrice && pair.quote.BidPrice >= pair.order.LimitPrice))
                                   .ToList();

        var accountIds = orderQuoteList.Select(pair => pair.order.AccountId)
                                       .Distinct()
                                       .ToList();

        var accountFilter = new AccountFilterQuery
                            {
                                Ids = accountIds
                            };

        var accountResponsePage = await m_UserServiceHttpClient.GetAllAccounts(accountFilter, Pageable.Create(1, accountIds.Count));

        // @formatter:off
        await Task.WhenAll(orderQuoteList.GroupJoin(accountResponsePage.Items, orderQuote => orderQuote.order.AccountId, accountResponse => accountResponse.Id,
                                                    (orderQuote, accountResponses) => new { orderQuote.quote, orderQuote.order, accountResponses })
                                         .SelectMany(group => group.accountResponses.Select(accountResponse => new { group.quote, group.order, accountResponse }))
                                         .Select(triple => m_UserServiceHttpClient.CreateTransaction(new TransactionCreateRequest
                                                                                                     {
                                                                                                         FromAccountNumber = triple.order.Direction == Direction.Buy ? triple.accountResponse.AccountNumber : null,
                                                                                                         FromCurrencyId    = triple.order.Direction == Direction.Buy ? triple.accountResponse.Currency.Id : triple.quote.Security!.StockExchange!.CurrencyId,
                                                                                                         ToAccountNumber   = triple.order.Direction == Direction.Sell ? triple.accountResponse.AccountNumber : null,
                                                                                                         ToCurrencyId      = triple.order.Direction == Direction.Sell ? triple.accountResponse.Currency.Id : triple.quote.Security!.StockExchange!.CurrencyId,
                                                                                                         Amount            = triple.order.Direction == Direction.Buy ? triple.order.RemainingPortions * triple.quote.AskPrice
                                                                                                                                                                     : triple.order.RemainingPortions * triple.quote.BidPrice,
                                                                                                         CodeId            = m_SecurityTransactionCodeId,
                                                                                                         Purpose           = "stock order"
                                                                                                     })));
        // @formatter:on

        await m_OrderRepository.UpdateStatus(orderQuoteList.Select(orderQuote => orderQuote.order.Id)
                                                           .ToList(), OrderStatus.Active);
    }

    public async Task ProcessForexQuotes(List<Quote> quotes) //TODO: check for values
    {
        var forexOrders = await m_RedisRepository.FindAllForexOrders();

        var orderQuoteList = quotes.GroupJoin(forexOrders, quote => quote.Security!.Ticker, order => order.Ticker, (quote, orders) => new { quote, orders })
                                   .SelectMany(group => group.orders.Select(order => new { group.quote, order }))
                                   .Where(pair => pair.order.OrderType is not OrderType.Market || pair.order.Direction is not Direction.Buy  || true)
                                   .Where(pair => pair.order.OrderType is not OrderType.Market || pair.order.Direction is not Direction.Sell || true)
                                   .Where(pair => pair.order.OrderType is not OrderType.Limit || pair.order.Direction is not Direction.Buy ||
                                                  pair.quote.BidPrice <= pair.order.LimitPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Limit || pair.order.Direction is not Direction.Sell ||
                                                  pair.quote.AskPrice >= pair.order.LimitPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Stop || pair.order.Direction is not Direction.Buy ||
                                                  pair.quote.BidPrice >= pair.order.StopPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.Stop || pair.order.Direction is not Direction.Sell ||
                                                  pair.quote.AskPrice <= pair.order.StopPrice)
                                   .Where(pair => pair.order.OrderType is not OrderType.StopLimit || pair.order.Direction is not Direction.Buy ||
                                                  (pair.quote.BidPrice >= pair.order.StopPrice && pair.quote.AskPrice <= pair.order.LimitPrice))
                                   .Where(pair => pair.order.OrderType is not OrderType.StopLimit || pair.order.Direction is not Direction.Sell ||
                                                  (pair.quote.AskPrice <= pair.order.StopPrice && pair.quote.BidPrice >= pair.order.LimitPrice))
                                   .ToList();

        var accountIds = orderQuoteList.Select(pair => pair.order.AccountId)
                                       .Distinct()
                                       .ToList();

        var accountFilter = new AccountFilterQuery
                            {
                                Ids = accountIds
                            };

        var accountResponsePage = await m_UserServiceHttpClient.GetAllAccounts(accountFilter, Pageable.Create(1, accountIds.Count));

        // @formatter:off
        await Task.WhenAll(orderQuoteList.GroupJoin(accountResponsePage.Items, orderQuote => orderQuote.order.AccountId, accountResponse => accountResponse.Id,
                                                    (orderQuote, accountResponses) => new { orderQuote.quote, orderQuote.order, accountResponses })
                                         .SelectMany(group => group.accountResponses.Select(accountResponse => new { group.quote, group.order, accountResponse }))
                                         .Select(triple => m_UserServiceHttpClient.CreateTransaction(new TransactionCreateRequest
                                                                                                     {
                                                                                                         FromAccountNumber = triple.order.Direction == Direction.Buy ? triple.accountResponse.AccountNumber : null,
                                                                                                         FromCurrencyId    = triple.order.Direction == Direction.Buy ? triple.accountResponse.Currency.Id : triple.quote.Security!.StockExchange!.CurrencyId,
                                                                                                         ToAccountNumber   = triple.order.Direction == Direction.Sell ? triple.accountResponse.AccountNumber : null,
                                                                                                         ToCurrencyId      = triple.order.Direction == Direction.Sell ? triple.accountResponse.Currency.Id : triple.quote.Security!.StockExchange!.CurrencyId,
                                                                                                         Amount            = triple.order.Direction == Direction.Buy ? triple.order.RemainingPortions * triple.quote.AskPrice
                                                                                                                                                                     : triple.order.RemainingPortions * triple.quote.BidPrice,
                                                                                                         CodeId            = m_SecurityTransactionCodeId,
                                                                                                         Purpose           = "forex order"
                                                                                                     })));
        // @formatter:on

        await m_OrderRepository.UpdateStatus(orderQuoteList.Select(orderQuote => orderQuote.order.Id)
                                                           .ToList(), OrderStatus.Active);
    }

    public Task ProcessOptionQuotes(List<Quote> quotes)
    {
        return Task.CompletedTask;
    }

    public Task OnApplicationStopped() => Task.CompletedTask;
}