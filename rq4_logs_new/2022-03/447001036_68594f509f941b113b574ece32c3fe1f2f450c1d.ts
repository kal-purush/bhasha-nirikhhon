import { TradierQuotes } from '../../../../src/types/tradier/quotes';
import { TradierHistory } from '../../../../src/types/tradier/history';
import { TradierSeries } from '../../../../src/types/tradier/timesales';
import * as Time from '@craigmiller160/ts-functions/es/Time';
import { CoinGeckoMarketChart } from '../../../../src/types/coingecko/marketchart';
import { CoinGeckoPrice } from '../../../../src/types/coingecko/price';
import MockAdapter from 'axios-mock-adapter';
import { MarketInvestmentInfo } from '../../../../src/types/data/MarketInvestmentInfo';
import { MarketTime } from '../../../../src/types/MarketTime';
import {
	TradierCalendar,
	TradierCalendarStatus
} from '../../../../src/types/tradier/calendar';
import { match, when } from 'ts-pattern';
import { PredicateT } from '@craigmiller160/ts-functions/es/types';
import {
	isCrypto,
	isStock
} from '../../../../src/types/data/MarketInvestmentType';
import {
	getFiveYearStartDate,
	getOneMonthStartDate,
	getOneWeekStartDate,
	getOneYearStartDate,
	getThreeMonthStartDate,
	getTodayEnd,
	getTodayEndString,
	getTodayStart,
	getTodayStartString
} from '../../../../src/utils/timeUtils';
import { pipe } from 'fp-ts/es6/function';

const TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
const DATE_FORMAT = 'yyyy-MM-dd';
const CALENDAR_MONTH_FORMAT = 'MM';
const CALENDAR_YEAR_FORMAT = 'yyyy';
const formatTimestamp = Time.format(TIMESTAMP_FORMAT);
const formatDate = Time.format(DATE_FORMAT);
const formatCalendarMonth = Time.format(CALENDAR_MONTH_FORMAT);
const formatCalendarYear = Time.format(CALENDAR_YEAR_FORMAT);

export const BASE_LAST_PRICE = 100;
export const BASE_PREV_CLOSE_PRICE = 30;
export const BASE_HISTORY_1_PRICE = 50;
export const BASE_HISTORY_2_PRICE = 60;

const createTradierQuote = (
	symbol: string,
	modifier: number
): TradierQuotes => ({
	quotes: {
		quote: {
			symbol,
			description: '',
			open: 0,
			high: 0,
			low: 0,
			bid: 0,
			ask: 0,
			close: 0,
			last: BASE_LAST_PRICE + modifier,
			prevclose: BASE_PREV_CLOSE_PRICE + modifier
		}
	}
});

const createTradierHistory = (modifier: number): TradierHistory => ({
	history: {
		day: [
			{
				date: '2022-01-01',
				open: BASE_HISTORY_1_PRICE + modifier,
				high: 0,
				low: 0,
				close: BASE_HISTORY_2_PRICE + modifier
			}
		]
	}
});

const getCryptoId = (symbol: string): string =>
	match(symbol)
		.with('BTC', () => 'bitcoin')
		.with('ETH', () => 'ethereum')
		.run();

const createTradierTimesale = (
	modifier: number,
	timestampMillis: number = new Date().getTime() - 120_000
): TradierSeries => {
	const secondTimestampMillis = timestampMillis + 60_000;
	const firstTime = formatTimestamp(new Date(timestampMillis));
	const secondTime = formatTimestamp(new Date(secondTimestampMillis));
	return {
		series: {
			data: [
				{
					time: firstTime,
					timestamp: Math.floor(timestampMillis / 1000),
					price: BASE_HISTORY_1_PRICE + modifier,
					open: 0,
					high: 0,
					low: 0,
					close: 0,
					volume: 0,
					vwap: 0
				},
				{
					time: secondTime,
					timestamp: Math.floor(secondTimestampMillis / 1000),
					price: BASE_HISTORY_2_PRICE + modifier,
					open: 0,
					high: 0,
					low: 0,
					close: 0,
					volume: 0,
					vwap: 0
				}
			]
		}
	};
};

const createCoinGeckoMarketChart = (
	modifier: number
): CoinGeckoMarketChart => ({
	prices: [
		[new Date().getTime(), BASE_HISTORY_1_PRICE + modifier],
		[new Date().getTime(), BASE_HISTORY_2_PRICE + modifier]
	]
});

const createCoinGeckoPrice = (
	id: string,
	modifier: number
): CoinGeckoPrice => ({
	[id]: {
		usd: BASE_LAST_PRICE + modifier
	}
});

export const createMockCalendar = (
	date: string,
	status: TradierCalendarStatus
): TradierCalendar => ({
	calendar: {
		month: 0,
		year: 0,
		days: {
			day: [
				{
					date,
					status
				}
			]
		}
	}
});

export interface MockApiConfig {
	readonly time: MarketTime;
	readonly status?: TradierCalendarStatus;
	readonly tradierTimesaleBaseMillis?: number;
}

const mockCalenderRequest = (
	mockApi: MockAdapter,
	status: TradierCalendarStatus
) => {
	const date = new Date();
	const formattedDate = formatDate(date);
	const year = formatCalendarYear(date);
	const month = formatCalendarMonth(date);

	mockApi
		.onGet(`/tradier/markets/calendar?year=${year}&month=${month}`)
		.reply(200, createMockCalendar(formattedDate, status));
};

const isStockInfo: PredicateT<MarketInvestmentInfo> = (info) =>
	isStock(info.type);
const isCryptoInfo: PredicateT<MarketInvestmentInfo> = (info) =>
	isCrypto(info.type);
const isNotToday: PredicateT<MarketTime> = (time) =>
	MarketTime.ONE_DAY !== time;
const isToday: PredicateT<MarketTime> = (time) => MarketTime.ONE_DAY === time;

const mockTradierQuoteRequest = (
	mockApi: MockAdapter,
	symbol: string,
	modifier: number
) => {
	mockApi
		.onGet(`/tradier/markets/quotes?symbols=${symbol}`)
		.reply(200, createTradierQuote(symbol, modifier));
};

const mockTradierTimesaleRequest = (
	mockApi: MockAdapter,
	symbol: string,
	modifier: number,
	timestampMillis?: number
) => {
	const start = getTodayStartString();
	const end = getTodayEndString();
	mockApi
		.onGet(
			`/tradier/markets/timesales?symbol=${symbol}&start=${start}&end=${end}&interval=1min`
		)
		.reply(200, createTradierTimesale(modifier, timestampMillis));
};

const getTradierInterval = (time: MarketTime): string =>
	match(time)
		.with(MarketTime.ONE_WEEK, () => 'daily')
		.with(MarketTime.ONE_MONTH, () => 'daily')
		.with(MarketTime.THREE_MONTHS, () => 'daily')
		.with(MarketTime.ONE_YEAR, () => 'weekly')
		.with(MarketTime.FIVE_YEARS, () => 'monthly')
		.run();

const getHistoryStart = (time: MarketTime): Date =>
	match(time)
		.with(MarketTime.ONE_DAY, getTodayStart)
		.with(MarketTime.ONE_WEEK, getOneWeekStartDate)
		.with(MarketTime.ONE_MONTH, getOneMonthStartDate)
		.with(MarketTime.THREE_MONTHS, getThreeMonthStartDate)
		.with(MarketTime.ONE_YEAR, getOneYearStartDate)
		.with(MarketTime.FIVE_YEARS, getFiveYearStartDate)
		.run();

const mockTradierHistoryRequest = (
	mockApi: MockAdapter,
	symbol: string,
	time: MarketTime,
	modifier: number
) => {
	const start = pipe(getHistoryStart(time), formatDate);
	const end = formatDate(new Date());
	const interval = getTradierInterval(time);
	mockApi
		.onGet(
			`/tradier/markets/history?symbol=${symbol}&start=${start}&end=${end}&interval=${interval}`
		)
		.reply(200, createTradierHistory(modifier));
};

const mockCoinGeckoPriceRequest = (
	mockApi: MockAdapter,
	symbol: string,
	modifier: number
) => {
	const id = getCryptoId(symbol);
	mockApi
		.onGet(`/coingecko/simple/price?ids=${id}&vs_currencies=usd`)
		.reply(200, createCoinGeckoPrice(id, modifier));
};

const millisToSecs = (millis: number): number => millis / 1000;

const mockCoinGeckoHistoryRequest = (
	mockApi: MockAdapter,
	symbol: string,
	time: MarketTime,
	modifier: number
) => {
	const id = getCryptoId(symbol);
	const startSecs = pipe(
		getHistoryStart(time).getTime(),
		millisToSecs,
		Math.floor
	).toString();
	const endSecs = pipe(
		getTodayEnd().getTime(),
		millisToSecs,
		Math.floor
	).toString();

	const startPattern = `${startSecs.substring(0, 8)}\\d\\d`;
	const endPattern = `${endSecs.substring(0, 8)}\\d\\d`;

	const urlPattern = RegExp(
		`\\/coingecko\\/coins\\/${id}\\/market_chart\\/range\\?vs_currency=usd&from=${startPattern}&to=${endPattern}`
	);

	mockApi.onGet(urlPattern).reply(200, createCoinGeckoMarketChart(modifier));
};

export const createSetupMockApiCalls =
	(
		mockApi: MockAdapter,
		investmentInfo: ReadonlyArray<MarketInvestmentInfo>
	) =>
	(config: MockApiConfig) => {
		mockCalenderRequest(mockApi, config.status ?? 'open');

		investmentInfo.forEach((info, index) => {
			match({ info, time: config.time })
				.with(
					{ info: when(isStockInfo), time: when(isNotToday) },
					() => {
						mockTradierQuoteRequest(mockApi, info.symbol, index);
						mockTradierHistoryRequest(
							mockApi,
							info.symbol,
							config.time,
							index
						);
					}
				)
				.with({ info: when(isStockInfo), time: when(isToday) }, () => {
					mockTradierQuoteRequest(mockApi, info.symbol, index);
					mockTradierTimesaleRequest(
						mockApi,
						info.symbol,
						index,
						config.tradierTimesaleBaseMillis
					);
				})
				.with({ info: when(isCryptoInfo) }, () => {
					mockCoinGeckoPriceRequest(mockApi, info.symbol, index);
					mockCoinGeckoHistoryRequest(
						mockApi,
						info.symbol,
						config.time,
						index
					);
				})
				.run();
		});
	};