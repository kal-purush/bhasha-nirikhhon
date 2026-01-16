import { hydrateLive } from '@app/actions/live';
import { LiveGame } from '@app/models/game';
import { ClockState } from '@app/reducers/clock';
import { LiveState } from '@app/reducers/live';
import { hydrateState, persistState, resetCache } from '@app/slices/live-store';
import { idb } from '@app/storage/idb-wrapper';
import { RootAction, RootState } from '@app/store';
import { expect } from '@open-wc/testing';
import { Store } from 'redux';
import * as sinon from 'sinon';
import { buildRunningTimer } from '../helpers/test-clock-data';
import { getLiveGameWithPlayers } from '../helpers/test-live-game-data';

const KEY_CACHED_LIVE = 'CACHED_LIVE';

function buildClock(): ClockState {
  return {
    timer: buildRunningTimer()
  }
}

function mockGetState(currentGame?: LiveGame, clock?: ClockState) {
  const liveState: LiveState = {
    hydrated: false,
    gameId: currentGame?.id || '',
    liveGame: currentGame,
    clock
  } as LiveState;
  return sinon.fake(() => {
    const mockState: RootState = {
      live: liveState,
    };
    return mockState;
  });
}

function mockStore(getStateMock: any): Store<RootState, RootAction> {
  return {
    getState: getStateMock,
    dispatch: sinon.stub(),
    replaceReducer: sinon.stub(),
    subscribe: sinon.stub(),
  } as unknown as Store<RootState, RootAction>;
}

describe('Live store', () => {
  let mockedIDBGet: sinon.SinonStub;
  let mockedIDBSet: sinon.SinonStub;

  beforeEach(() => {
    // Resets the cache to avoid data from other tests.
    resetCache();

    mockedIDBGet = sinon.stub(idb, 'get').resolves(undefined);
    mockedIDBSet = sinon.stub(idb, 'set').resolves();
  });

  afterEach(() => {
    sinon.restore();
  });

  describe('hydrateState', () => {

    it('should do nothing if idb is empty', () => {
      const getStateMock = mockGetState();
      const storeMock = mockStore(getStateMock);

      hydrateState(storeMock);

      expect(mockedIDBGet).to.have.callCount(1);
      expect(getStateMock).to.not.have.been.called;
      expect(storeMock.dispatch).to.not.have.been.called;
    });

    it('should populate the live state when found in idb', async () => {
      const currentGame = getLiveGameWithPlayers();
      const currentClock = buildClock();
      const cachedData = {
        currentGameId: currentGame.id,
        game: { ...currentGame },
        clock: { ...currentClock },
      };
      mockedIDBGet.onFirstCall().resolves(cachedData);

      const getStateMock = mockGetState();
      const storeMock = mockStore(getStateMock);

      hydrateState(storeMock);

      // Waits for promises to resolve.
      await Promise.resolve();

      const hydrateAction = hydrateLive(cachedData.game, cachedData.currentGameId, cachedData.clock);

      expect(storeMock.dispatch).to.have.been.calledWith(hydrateAction);
    });

  }); // describe('hydrateState')

  describe('persistState', () => {

    it('should do nothing if current game is missing from state', () => {
      const getStateMock = mockGetState();
      const storeMock = mockStore(getStateMock);

      persistState(storeMock);

      expect(getStateMock).to.have.callCount(1);

      expect(storeMock.dispatch).to.not.have.been.called;
      expect(mockedIDBSet).to.not.have.been.called;
    });

    it('should cache the live state when changed', async () => {
      const currentGame = getLiveGameWithPlayers();
      const currentClock = buildClock();
      const getStateMock = mockGetState(currentGame, currentClock);
      const storeMock = mockStore(getStateMock);

      persistState(storeMock);

      // Waits for promises to resolve.
      await Promise.resolve();

      expect(getStateMock).to.have.callCount(1);

      expect(storeMock.dispatch).to.not.have.been.called;

      const expectedCachedData = {
        currentGameId: currentGame.id,
        game: { ...currentGame },
        clock: { ...currentClock },
      };
      expect(mockedIDBSet).to.have.been.calledWith(KEY_CACHED_LIVE, expectedCachedData);
    });

    it('should do nothing if live state is already cached', async () => {
      const currentGame = getLiveGameWithPlayers();
      const currentClock = buildClock();
      const getStateMock = mockGetState(currentGame, currentClock);
      const storeMock = mockStore(getStateMock);

      // Call persist once, to setup the game in cache.
      persistState(storeMock);

      // Waits for promises to resolve.
      await Promise.resolve();

      expect(getStateMock, 'First getState call').to.have.callCount(1);
      expect(mockedIDBSet, 'First idb.set call').to.have.callCount(1);

      // Next call to persist should not update the cache.
      persistState(storeMock);

      // Waits for promises to resolve.
      await Promise.resolve();

      expect(getStateMock, 'Second getState call').to.have.callCount(2);
      expect(mockedIDBSet, 'Second idb.set call').to.have.callCount(1);
      expect(storeMock.dispatch).to.not.have.been.called;
    });

  }); // describe('persistState')

}); // describe('Live store')