namespace ShareInvest.Utilities.Kiwoom;

public enum Revise
{
    유상증자 = 1,
    무상증자 = 2,
    배당락 = 4,
    액면분할 = 8,
    액면병합 = 16,
    기업합병 = 32,
    감자 = 64,
    권리락 = 256
}
public enum ChartDuration
{
    DailyChart = 'D'
}
public enum MarketOperation
{
    장시작전 = 0,
    장마감전_동시호가 = 2,
    장시작 = 3,
    장종료_예상지수종료 = 4,
    장마감 = 8,
    장종료_시간외종료 = 9,
    시간외_종가매매_시작 = 'a',
    시간외_종가매매_종료 = 'b',
    시간외_단일가_매매시작 = 'c',
    시간외_단일가_매매종료 = 'd',
    선옵_장마감전_동시호가_시작 = 's',
    선옵_장마감전_동시호가_종료 = 'e',
    선옵_조기개장_알림 = 'f'
}
public enum RealType
{
    주식시세 = 19,
    주식체결 = 25,
    주식우선호가 = 2,
    주식호가잔량 = 103,
    주식시간외호가 = 5,
    주식당일거래원 = 57,
    ETF_NAV = 15 + 'e',
    ELW_지표 = 6 + 'e',
    ELW_이론가 = 10 + 'e',
    주식예상체결 = 7,
    주식종목정보 = 11,
    선물옵션우선호가 = 3 + 'f',
    선물시세 = 32 + 'f',
    선물호가잔량 = 60 + 'f',
    선물이론가 = 10 + 'f',
    옵션시세 = 42 + 'o',
    옵션호가잔량 = 60 + 'o',
    옵션이론가 = 15 + 'o',
    업종지수 = 12,
    업종등락 = 14,
    장시작시간 = 3,
    VI발동_해제 = 19 + 'v',
    종목프로그램매매 = 17
}
public enum ChejanType
{
    주문체결 = 0x30,
    잔고 = 49,
    파생잔고 = 52
}
public enum OrderState
{
    접수,
    체결,
    취소
}
public enum OrderStatus
{
    매도 = 1,
    매수 = 2
}
public static class Operation
{
    public static MarketOperation Get(string? arg)
    {
        if (arg?.Length == 1)
        {
            var index = char.IsDigit(arg[0]) ? Convert.ToInt32(arg) : Convert.ToChar(arg);

            if (Enum.IsDefined(typeof(MarketOperation), index))
            {
                return (MarketOperation)index;
            }
        }
        return MarketOperation.장종료_시간외종료;
    }
}