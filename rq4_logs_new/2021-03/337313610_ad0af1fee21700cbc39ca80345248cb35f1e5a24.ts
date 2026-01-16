import * as $protobuf from "protobufjs";
/** Namespace com. */
export namespace com {

    /** Namespace jleoirab. */
    namespace jleoirab {

        /** Namespace xando. */
        namespace xando {

            /** Namespace events. */
            namespace events {

                /** Properties of a MoveEvent. */
                interface IMoveEvent {

                    /** MoveEvent gamePlayer */
                    gamePlayer?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** MoveEvent cellIndex */
                    cellIndex?: (number|null);

                    /** MoveEvent playerTag */
                    playerTag?: (com.jleoirab.xando.protos.PlayerTag|null);
                }

                /** Represents a MoveEvent. */
                class MoveEvent implements IMoveEvent {

                    /**
                     * Constructs a new MoveEvent.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.events.IMoveEvent);

                    /** MoveEvent gamePlayer. */
                    public gamePlayer?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** MoveEvent cellIndex. */
                    public cellIndex: number;

                    /** MoveEvent playerTag. */
                    public playerTag: com.jleoirab.xando.protos.PlayerTag;

                    /**
                     * Creates a new MoveEvent instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns MoveEvent instance
                     */
                    public static create(properties?: com.jleoirab.xando.events.IMoveEvent): com.jleoirab.xando.events.MoveEvent;

                    /**
                     * Encodes the specified MoveEvent message. Does not implicitly {@link com.jleoirab.xando.events.MoveEvent.verify|verify} messages.
                     * @param message MoveEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.events.IMoveEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified MoveEvent message, length delimited. Does not implicitly {@link com.jleoirab.xando.events.MoveEvent.verify|verify} messages.
                     * @param message MoveEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.events.IMoveEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a MoveEvent message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns MoveEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.events.MoveEvent;

                    /**
                     * Decodes a MoveEvent message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns MoveEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.events.MoveEvent;

                    /**
                     * Verifies a MoveEvent message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a MoveEvent message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns MoveEvent
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.events.MoveEvent;

                    /**
                     * Creates a plain object from a MoveEvent message. Also converts values to other types if specified.
                     * @param message MoveEvent
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.events.MoveEvent, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this MoveEvent to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }

                /** Properties of a JoinGameEvent. */
                interface IJoinGameEvent {

                    /** JoinGameEvent gamePlayer */
                    gamePlayer?: (com.jleoirab.xando.protos.IGamePlayer|null);
                }

                /** Represents a JoinGameEvent. */
                class JoinGameEvent implements IJoinGameEvent {

                    /**
                     * Constructs a new JoinGameEvent.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.events.IJoinGameEvent);

                    /** JoinGameEvent gamePlayer. */
                    public gamePlayer?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /**
                     * Creates a new JoinGameEvent instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns JoinGameEvent instance
                     */
                    public static create(properties?: com.jleoirab.xando.events.IJoinGameEvent): com.jleoirab.xando.events.JoinGameEvent;

                    /**
                     * Encodes the specified JoinGameEvent message. Does not implicitly {@link com.jleoirab.xando.events.JoinGameEvent.verify|verify} messages.
                     * @param message JoinGameEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.events.IJoinGameEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified JoinGameEvent message, length delimited. Does not implicitly {@link com.jleoirab.xando.events.JoinGameEvent.verify|verify} messages.
                     * @param message JoinGameEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.events.IJoinGameEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a JoinGameEvent message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns JoinGameEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.events.JoinGameEvent;

                    /**
                     * Decodes a JoinGameEvent message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns JoinGameEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.events.JoinGameEvent;

                    /**
                     * Verifies a JoinGameEvent message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a JoinGameEvent message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns JoinGameEvent
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.events.JoinGameEvent;

                    /**
                     * Creates a plain object from a JoinGameEvent message. Also converts values to other types if specified.
                     * @param message JoinGameEvent
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.events.JoinGameEvent, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this JoinGameEvent to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }

                /** Properties of a GameEvent. */
                interface IGameEvent {

                    /** GameEvent moveEvent */
                    moveEvent?: (com.jleoirab.xando.events.IMoveEvent|null);

                    /** GameEvent joinGameEvent */
                    joinGameEvent?: (com.jleoirab.xando.events.IJoinGameEvent|null);

                    /** GameEvent game */
                    game?: (com.jleoirab.xando.protos.IGame|null);
                }

                /** Represents a GameEvent. */
                class GameEvent implements IGameEvent {

                    /**
                     * Constructs a new GameEvent.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.events.IGameEvent);

                    /** GameEvent moveEvent. */
                    public moveEvent?: (com.jleoirab.xando.events.IMoveEvent|null);

                    /** GameEvent joinGameEvent. */
                    public joinGameEvent?: (com.jleoirab.xando.events.IJoinGameEvent|null);

                    /** GameEvent game. */
                    public game?: (com.jleoirab.xando.protos.IGame|null);

                    /** GameEvent event. */
                    public event?: ("moveEvent"|"joinGameEvent");

                    /**
                     * Creates a new GameEvent instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns GameEvent instance
                     */
                    public static create(properties?: com.jleoirab.xando.events.IGameEvent): com.jleoirab.xando.events.GameEvent;

                    /**
                     * Encodes the specified GameEvent message. Does not implicitly {@link com.jleoirab.xando.events.GameEvent.verify|verify} messages.
                     * @param message GameEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.events.IGameEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified GameEvent message, length delimited. Does not implicitly {@link com.jleoirab.xando.events.GameEvent.verify|verify} messages.
                     * @param message GameEvent message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.events.IGameEvent, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a GameEvent message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns GameEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.events.GameEvent;

                    /**
                     * Decodes a GameEvent message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns GameEvent
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.events.GameEvent;

                    /**
                     * Verifies a GameEvent message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a GameEvent message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns GameEvent
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.events.GameEvent;

                    /**
                     * Creates a plain object from a GameEvent message. Also converts values to other types if specified.
                     * @param message GameEvent
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.events.GameEvent, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this GameEvent to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }
            }

            /** Namespace protos. */
            namespace protos {

                /** Properties of a GamePlayer. */
                interface IGamePlayer {

                    /** GamePlayer playerId */
                    playerId?: (string|null);

                    /** GamePlayer playerName */
                    playerName?: (string|null);
                }

                /** Represents a GamePlayer. */
                class GamePlayer implements IGamePlayer {

                    /**
                     * Constructs a new GamePlayer.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.protos.IGamePlayer);

                    /** GamePlayer playerId. */
                    public playerId: string;

                    /** GamePlayer playerName. */
                    public playerName: string;

                    /**
                     * Creates a new GamePlayer instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns GamePlayer instance
                     */
                    public static create(properties?: com.jleoirab.xando.protos.IGamePlayer): com.jleoirab.xando.protos.GamePlayer;

                    /**
                     * Encodes the specified GamePlayer message. Does not implicitly {@link com.jleoirab.xando.protos.GamePlayer.verify|verify} messages.
                     * @param message GamePlayer message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.protos.IGamePlayer, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified GamePlayer message, length delimited. Does not implicitly {@link com.jleoirab.xando.protos.GamePlayer.verify|verify} messages.
                     * @param message GamePlayer message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.protos.IGamePlayer, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a GamePlayer message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns GamePlayer
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.protos.GamePlayer;

                    /**
                     * Decodes a GamePlayer message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns GamePlayer
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.protos.GamePlayer;

                    /**
                     * Verifies a GamePlayer message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a GamePlayer message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns GamePlayer
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.protos.GamePlayer;

                    /**
                     * Creates a plain object from a GamePlayer message. Also converts values to other types if specified.
                     * @param message GamePlayer
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.protos.GamePlayer, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this GamePlayer to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }

                /** Properties of a GameBoard. */
                interface IGameBoard {

                    /** GameBoard cell */
                    cell?: (com.jleoirab.xando.protos.GameBoard.GameBoardCell[]|null);
                }

                /** Represents a GameBoard. */
                class GameBoard implements IGameBoard {

                    /**
                     * Constructs a new GameBoard.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.protos.IGameBoard);

                    /** GameBoard cell. */
                    public cell: com.jleoirab.xando.protos.GameBoard.GameBoardCell[];

                    /**
                     * Creates a new GameBoard instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns GameBoard instance
                     */
                    public static create(properties?: com.jleoirab.xando.protos.IGameBoard): com.jleoirab.xando.protos.GameBoard;

                    /**
                     * Encodes the specified GameBoard message. Does not implicitly {@link com.jleoirab.xando.protos.GameBoard.verify|verify} messages.
                     * @param message GameBoard message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.protos.IGameBoard, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified GameBoard message, length delimited. Does not implicitly {@link com.jleoirab.xando.protos.GameBoard.verify|verify} messages.
                     * @param message GameBoard message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.protos.IGameBoard, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a GameBoard message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns GameBoard
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.protos.GameBoard;

                    /**
                     * Decodes a GameBoard message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns GameBoard
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.protos.GameBoard;

                    /**
                     * Verifies a GameBoard message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a GameBoard message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns GameBoard
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.protos.GameBoard;

                    /**
                     * Creates a plain object from a GameBoard message. Also converts values to other types if specified.
                     * @param message GameBoard
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.protos.GameBoard, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this GameBoard to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }

                namespace GameBoard {

                    /** GameBoardCell enum. */
                    enum GameBoardCell {
                        UNKNOWN = 0,
                        UNOCCUPIED = 1,
                        X = 3,
                        O = 4
                    }
                }

                /** PlayerTag enum. */
                enum PlayerTag {
                    PLAYER_TAG_UNKNOWN = 0,
                    PLAYER_TAG_X = 1,
                    PLAYER_TAG_O = 2
                }

                /** GameState enum. */
                enum GameState {
                    GAME_STATE_UNKNOWN = 0,
                    GAME_STATE_CREATED = 1,
                    GAME_STATE_IN_PROGRESS = 2,
                    GAME_STATE_FINISHED = 3
                }

                /** Properties of a GameStatus. */
                interface IGameStatus {

                    /** GameStatus state */
                    state?: (com.jleoirab.xando.protos.GameState|null);

                    /** GameStatus winner */
                    winner?: (com.jleoirab.xando.protos.PlayerTag|null);
                }

                /** Represents a GameStatus. */
                class GameStatus implements IGameStatus {

                    /**
                     * Constructs a new GameStatus.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.protos.IGameStatus);

                    /** GameStatus state. */
                    public state: com.jleoirab.xando.protos.GameState;

                    /** GameStatus winner. */
                    public winner: com.jleoirab.xando.protos.PlayerTag;

                    /**
                     * Creates a new GameStatus instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns GameStatus instance
                     */
                    public static create(properties?: com.jleoirab.xando.protos.IGameStatus): com.jleoirab.xando.protos.GameStatus;

                    /**
                     * Encodes the specified GameStatus message. Does not implicitly {@link com.jleoirab.xando.protos.GameStatus.verify|verify} messages.
                     * @param message GameStatus message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.protos.IGameStatus, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified GameStatus message, length delimited. Does not implicitly {@link com.jleoirab.xando.protos.GameStatus.verify|verify} messages.
                     * @param message GameStatus message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.protos.IGameStatus, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a GameStatus message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns GameStatus
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.protos.GameStatus;

                    /**
                     * Decodes a GameStatus message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns GameStatus
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.protos.GameStatus;

                    /**
                     * Verifies a GameStatus message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a GameStatus message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns GameStatus
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.protos.GameStatus;

                    /**
                     * Creates a plain object from a GameStatus message. Also converts values to other types if specified.
                     * @param message GameStatus
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.protos.GameStatus, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this GameStatus to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }

                /** Properties of a Game. */
                interface IGame {

                    /** Game uid */
                    uid?: (string|null);

                    /** Game id */
                    id?: (string|null);

                    /** Game gameCreatorPlayerId */
                    gameCreatorPlayerId?: (string|null);

                    /** Game playerX */
                    playerX?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** Game playerO */
                    playerO?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** Game gameBoard */
                    gameBoard?: (com.jleoirab.xando.protos.IGameBoard|null);

                    /** Game currentPlayerTurn */
                    currentPlayerTurn?: (com.jleoirab.xando.protos.PlayerTag|null);

                    /** Game gameStatus */
                    gameStatus?: (com.jleoirab.xando.protos.IGameStatus|null);
                }

                /** Represents a Game. */
                class Game implements IGame {

                    /**
                     * Constructs a new Game.
                     * @param [properties] Properties to set
                     */
                    constructor(properties?: com.jleoirab.xando.protos.IGame);

                    /** Game uid. */
                    public uid: string;

                    /** Game id. */
                    public id: string;

                    /** Game gameCreatorPlayerId. */
                    public gameCreatorPlayerId: string;

                    /** Game playerX. */
                    public playerX?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** Game playerO. */
                    public playerO?: (com.jleoirab.xando.protos.IGamePlayer|null);

                    /** Game gameBoard. */
                    public gameBoard?: (com.jleoirab.xando.protos.IGameBoard|null);

                    /** Game currentPlayerTurn. */
                    public currentPlayerTurn: com.jleoirab.xando.protos.PlayerTag;

                    /** Game gameStatus. */
                    public gameStatus?: (com.jleoirab.xando.protos.IGameStatus|null);

                    /**
                     * Creates a new Game instance using the specified properties.
                     * @param [properties] Properties to set
                     * @returns Game instance
                     */
                    public static create(properties?: com.jleoirab.xando.protos.IGame): com.jleoirab.xando.protos.Game;

                    /**
                     * Encodes the specified Game message. Does not implicitly {@link com.jleoirab.xando.protos.Game.verify|verify} messages.
                     * @param message Game message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encode(message: com.jleoirab.xando.protos.IGame, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Encodes the specified Game message, length delimited. Does not implicitly {@link com.jleoirab.xando.protos.Game.verify|verify} messages.
                     * @param message Game message or plain object to encode
                     * @param [writer] Writer to encode to
                     * @returns Writer
                     */
                    public static encodeDelimited(message: com.jleoirab.xando.protos.IGame, writer?: $protobuf.Writer): $protobuf.Writer;

                    /**
                     * Decodes a Game message from the specified reader or buffer.
                     * @param reader Reader or buffer to decode from
                     * @param [length] Message length if known beforehand
                     * @returns Game
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): com.jleoirab.xando.protos.Game;

                    /**
                     * Decodes a Game message from the specified reader or buffer, length delimited.
                     * @param reader Reader or buffer to decode from
                     * @returns Game
                     * @throws {Error} If the payload is not a reader or valid buffer
                     * @throws {$protobuf.util.ProtocolError} If required fields are missing
                     */
                    public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): com.jleoirab.xando.protos.Game;

                    /**
                     * Verifies a Game message.
                     * @param message Plain object to verify
                     * @returns `null` if valid, otherwise the reason why it is not
                     */
                    public static verify(message: { [k: string]: any }): (string|null);

                    /**
                     * Creates a Game message from a plain object. Also converts values to their respective internal types.
                     * @param object Plain object
                     * @returns Game
                     */
                    public static fromObject(object: { [k: string]: any }): com.jleoirab.xando.protos.Game;

                    /**
                     * Creates a plain object from a Game message. Also converts values to other types if specified.
                     * @param message Game
                     * @param [options] Conversion options
                     * @returns Plain object
                     */
                    public static toObject(message: com.jleoirab.xando.protos.Game, options?: $protobuf.IConversionOptions): { [k: string]: any };

                    /**
                     * Converts this Game to JSON.
                     * @returns JSON object
                     */
                    public toJSON(): { [k: string]: any };
                }
            }
        }
    }
}