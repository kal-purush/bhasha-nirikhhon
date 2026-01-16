import { Game, GameId, PlayerConnectionStatus } from '@machikoro/game-server-contracts';
import {
  push,
  ref,
  set,
  onDisconnect,
  serverTimestamp,
  Database,
} from 'firebase/database';

import { AbandonGame } from '../game';
import { CreateGame } from '../lobby';

export const createFirebaseGame = (firebaseDb: Database): CreateGame => async (lobby, hostId) => {
  // TODO?: probably should be inside cloud function
  const playersConnectionStatuses = Object.fromEntries(
    Object.keys(lobby.users!).map((userId) => [userId, PlayerConnectionStatus.DISCONNECTED]),
  );

  const gameToCreate: Omit<Game, 'gameId'> = {
    hostId,
    players: lobby.users!,
    playersConnectionStatuses,
  };

  const createdGameRef = await push(ref(firebaseDb, 'games'), {
    ...gameToCreate,
    createdAt: serverTimestamp(),
  });

  // This cast is safe because only root database has a `null` key
  const gameId = createdGameRef.key! as GameId;

  const lobbyGameIdRef = ref(firebaseDb, `lobbies/${lobby.lobbyId}/gameId`);

  await set(lobbyGameIdRef, gameId);

  return {
    ...gameToCreate,
    gameId,
  };
};

export const joinFirebaseGame = (firebaseDb: Database): AbandonGame => async (user, gameId) => {
  const gamePlayerStatusRef = ref(firebaseDb, `games/${gameId}/playersConnectionStatuses/${user.userId}`);

  await set(gamePlayerStatusRef, PlayerConnectionStatus.CONNECTED);

  void onDisconnect(gamePlayerStatusRef).set(PlayerConnectionStatus.DISCONNECTED);

  return gameId;
};

export const abandonFirebaseGame = (firebaseDb: Database): AbandonGame => async (user, gameId) => {
  const gamePlayerStatusRef = ref(firebaseDb, `games/${gameId}/playersConnectionStatuses/${user.userId}`);

  await set(gamePlayerStatusRef, PlayerConnectionStatus.ABANDONED);

  return gameId;
};