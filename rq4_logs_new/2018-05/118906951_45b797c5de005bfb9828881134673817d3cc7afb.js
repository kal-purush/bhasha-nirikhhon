module.exports = {
    BASE: `https://api.huobipro.com`,
    URL: {
        ws: `wss://api.huobipro.com/ws`,
        klineHistory: `/market/history/kline`,//获取K线数据
        detail: `/market/detail/merged`,//获取聚合行情(Ticker)
        depthMarket: `/market/depth`,//获取 Market Depth 数据
        tradeMarket: `/market/trade`,//获取 Trade Detail 数据
        tradeHistory: `/market/history/trade`,//批量获取最近的交易记录
        marketDetail: `/market/detail`,//获取 Market Detail 24小时成交量数据
        querySymbols: `/v1/common/symbols`,//查询Pro站支持的所有交易对及精度
        queryCurrencys: `/v1/common/currencys`,//查询Pro站支持的所有币种
        queryTimestamp: `/v1/common/timestamp`,//查询系统当前时间
        accounts: `/v1/account/accounts`,//查询当前用户的所有账户(即account-id)
        //accountBalance: `/v1/account/accounts/${config.huobi.account_id}/balance`,/// 查询Pro站指定账户的余额
        placeOrder: `/v1/order/orders/place`,//Pro站下单
        batchcancel: `/v1/order/orders/batchcancel`, //批量撤销订单
        orders: `/v1/order/orders/`, //查询当前委托、历史委托   查询某个订单详情=/v1/order/orders/{order-id}
        //getOrders: `/v1/order/orders`,// 查询当前委托、历史委托
        //matchresults: `/v1/order/orders/{order-id}/matchresults`,// 查询某个订单的成交明细
        matchresults: `/v1/order/matchresults`, //查询当前成交、历史成交   查询某个订单的成交明细=/v1/order/orders/{order-id}/matchresults
    }
}