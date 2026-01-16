import { WebSocketGateway, SubscribeMessage, WebSocketServer, WsResponse, OnGatewayConnection } from '@nestjs/websockets';
import { Observable } from 'rxjs';
import { switchMap, map, tap } from 'rxjs/operators';

import { environment } from '@environment';
import Player from '../model/player';
import Game from '../model/game';

import { GameService, Movement } from '../services/game/game.service';
import { MutexService } from '../services/mutex/mutex.service';
import { RedisService } from '../services/redis/redis.service';
import { TokenService } from '../../services/token/token.service';

@WebSocketGateway(environment.ports.ws, { path: '/game' })
export class GameWebSocket implements OnGatewayConnection {
  /** Server reference. */
  @WebSocketServer() private readonly server;

  constructor(private readonly mutex: MutexService,
              private readonly redis: RedisService,
              private readonly logic: GameService,
              private readonly auth: TokenService) {}

  /** Authentification test. */
  async handleConnection(socket) {
    const token = socket.handshake.query.token;

    if (token) {
      // TODO: Decrypt token.
      // TODO: test if exist in database.
      console.log(token);
    } else {
      socket.client.close();
    }

    /****************************************
     * @see https://github.com/nestjs/nest/issues/1254
     * @see https://github.com/afertil/nest-chat-api/blob/master/src/modules/chat/chat.gateway.ts
     */
  }

  @SubscribeMessage('movement')
  movement(client, body: Movement): Observable<WsResponse<Game>> {
    return this.execute(this.logic.move, body).pipe(
      map(game => ({
        event: 'movement',
        data: game
      }))
    );
  }

  /**
   * Execute the given logic.
   * @template D
   * @param logic - Special function.
   * @param data - Parameters.
   * @returns {Observable<Game>}
   */
  private execute<D>(logic: (p: Player, g: Game, d: D) => Game, data: D): Observable<Game> {
    const player: Player = this.findPlayer();

    return this.access(player).pipe(
      map(game => logic(player, game, data)),
      switchMap(game => this.redis.setGameState(game)),
      tap(() => this.mutex.unlock())
    );
  }

  // TODO: Change.
  private findPlayer(): Player {
    return {
      id: 10,
      uuid: 'fsdqf',
      position: {
        x: 1,
        y: 8
      },
      items: [{ name: 'Potion' }]
    };
  }

  /**
   * Access to the game state.
   * @param {Player} player
   * @returns {Observable<Game>}
   */
  private access(player: Player): Observable<Game> {
    return this.mutex.lock().pipe(
      switchMap(() => this.redis.getGameState())
    );
  }
}