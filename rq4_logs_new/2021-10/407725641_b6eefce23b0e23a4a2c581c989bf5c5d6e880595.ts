
import store from './index';
import {feedActions} from './feed';
import { initialFeedState, initialFilteredFeed, initialResult } from './mocks';
import { Markets } from '../models';

test('should have initial data when initialized', () => {
   const {bid, ask, selectedMarket} = store.getState().feed;

   expect(bid.highestBidPrice).toBe(initialFeedState.bid.highestBidPrice)
   expect(bid.lowestBidPrice).toBe(initialFeedState.bid.lowestBidPrice)
   expect(bid.map).toEqual(initialFeedState.bid.map)
   expect(bid.depthArray).toEqual(initialFeedState.bid.depthArray)
   expect(bid.list).toEqual(initialFeedState.bid.list)

   expect(ask.highestAskPrice).toBe(initialFeedState.ask.highestAskPrice)
   expect(ask.lowestAskPrice).toBe(initialFeedState.ask.lowestAskPrice)
   expect(ask.map).toEqual(initialFeedState.ask.map)
   expect(ask.depthArray).toEqual(initialFeedState.ask.depthArray)
   expect(ask.list).toEqual(initialFeedState.ask.list)

   expect(selectedMarket).toBe(initialFeedState.selectedMarket);
});

test('should output transformed feed', () => {
   store.dispatch(feedActions.processSocketData({bids:initialFilteredFeed.asks}));
   store.dispatch(feedActions.processSocketData({asks:initialFilteredFeed.bids}));

   const {bid, ask} = store.getState().feed;

   expect(bid.highestBidPrice).toBe(initialResult.bids.highestBidPrice)
   expect(bid.lowestBidPrice).toBe(initialResult.bids.lowestBidPrice)
   expect(bid.map).toEqual(initialResult.bids.map)
   expect(bid.depthArray).toEqual(initialResult.bids.depthArray)
   expect(bid.list).toEqual(initialResult.bids.list)

   expect(ask.highestAskPrice).toBe(initialResult.asks.highestAskPrice)
   expect(ask.lowestAskPrice).toBe(initialResult.asks.lowestAskPrice)
   expect(ask.map).toEqual(initialResult.asks.map)
   expect(ask.depthArray).toEqual(initialResult.asks.depthArray)
   expect(ask.list).toEqual(initialResult.asks.list)
});

test('should change market to ETHEREUM', () => {
   store.dispatch(feedActions.changeMarket({selectedMarket:Markets.ETH_USD}));

   const {bid, ask, selectedMarket} = store.getState().feed;
   
   expect(bid.highestBidPrice).toBe(initialFeedState.bid.highestBidPrice)
   expect(bid.lowestBidPrice).toBe(initialFeedState.bid.lowestBidPrice)
   expect(bid.map).toEqual(initialFeedState.bid.map)
   expect(bid.depthArray).toEqual(initialFeedState.bid.depthArray)
   expect(bid.list).toEqual(initialFeedState.bid.list)

   expect(ask.highestAskPrice).toBe(initialFeedState.ask.highestAskPrice)
   expect(ask.lowestAskPrice).toBe(initialFeedState.ask.lowestAskPrice)
   expect(ask.map).toEqual(initialFeedState.ask.map)
   expect(ask.depthArray).toEqual(initialFeedState.ask.depthArray)
   expect(ask.list).toEqual(initialFeedState.ask.list)

   expect(selectedMarket).toBe(Markets.ETH_USD);
});