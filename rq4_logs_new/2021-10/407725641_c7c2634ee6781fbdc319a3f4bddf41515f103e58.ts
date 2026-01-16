
import store from '../index';
import {socketActions} from '../socket';

test('should have initial state', () => {
   const {isConnected, isSubscribed} = store.getState().socket;

   expect(isConnected).toBe(false);
   expect(isSubscribed).toBe(false);
});

test('should change state to connect', () => {
   store.dispatch(socketActions.connectSuccess());

   const {isConnected} = store.getState().socket;

   expect(isConnected).toBe(true);
});

test('should change state to subscribed', () => {
   store.dispatch(socketActions.subscribeSuccess());

   const {isConnected, isSubscribed} = store.getState().socket;

   expect(isConnected).toBe(true);
   expect(isSubscribed).toBe(true);
});

test('should change state to unsuscribed', () => {
   store.dispatch(socketActions.unsubscribeSuccess());

   const {isConnected, isSubscribed} = store.getState().socket;

   expect(isConnected).toBe(true);
   expect(isSubscribed).toBe(false);
});

test('should change state to unsubscribed', () => {
   store.dispatch(socketActions.unsubscribeSuccess());

   const {isConnected,isSubscribed} = store.getState().socket;
   
   expect(isConnected).toBe(true);
   expect(isSubscribed).toBe(false);
});

test('should change state to disconnected and unsubscribed on disconnect', () => {
   store.dispatch(socketActions.subscribeSuccess());
   store.dispatch(socketActions.disconnectSuccess());

   const {isConnected,isSubscribed} = store.getState().socket;
   
   expect(isSubscribed).toBe(false);
   expect(isConnected).toBe(false);
});