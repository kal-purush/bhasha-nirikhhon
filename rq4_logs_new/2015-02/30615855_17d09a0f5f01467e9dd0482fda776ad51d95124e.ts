export class PlayerList {

    private config: AppConfig;

    private players: Player[];

    constructor(config: AppConfig) {
        this.config = config;

        this.players = [];
    }

    public createPlayer(socket: SocketIO.Socket): Player {
        var player = new Player(socket);
        this.players[player.getID()] = player;
        return player;
    }

    public getPlayer(hash: string): Player {
        return this.players[hash];
    }

    public deletePlayer(hash: string): void {
        delete this.players[hash];
    }

}

export class Player {

    private socket: SocketIO.Socket;

    public constructor(socket: SocketIO.Socket) {
        this.socket = socket;
    }

    public getID(): string {
        return this.socket.id;
    }

}