import {Collection, Gossip, Storage} from '../services/_';
import {Computer, State} from '../types/_';
import { Express } from 'express';

export class Inform extends Gossip<State> {
    /**
     * Class construtor.
     *
     * @param computers The known computers in the network.
     * @param interval The interval at which this computer should gossip.
     * @param me This computer.
     * @param server The active express server.
     * @param storage
     */
    constructor(
        protected computers: Collection<Computer>,
        interval: number,
        me: Computer,
        server: Express,
        private storage: Storage,
    ) {
        //
        super(computers, 'inform', interval, me, server);

        this.helpers.addStates();
    }

    /**
     * This array contains all the keys the class does not wish to be send over
     * the network or to be used in equality comparisons.
     */
    public except(): string[] {
        //
        return ['date'];
    }

    /**
     * This method is called everytime the gossip service initiates a new
     * handshake with a randomly selected computer.
     */
    public onTick(): void {
        console.log(this.items)
    }

    /**
     * This method is called whenever the gossip service has received items that
     * it did not already know.
     *
     * @param items The newly received items.
     * @param last The last item the other computer has created.
     */
    public async onItems(items: State[], last: State): Promise<void> {
        //
        for (const item of items) {
            // We only accept items we don't already know.
            if (!this.helpers.isUnknown(item)) continue;

            this.items.add(item);
            await this.storage.states.create(item);
        }
    }

    /**
     * This method is called whenever the axios post request to another computer
     * throws an error.
     *
     * @param kind The kind of error that was thrown.
     * @param computer The computer to which the gossip service was communicating.
     * @param error The actual error object.
     */
    public onError(kind: string, computer: Computer, error?: Error): void {
        //
        console.log(kind, computer.ip, error.message);
    }

    /**
     * The helper methods.
     */
    private readonly helpers = {
        /**
         * Fills the items list with the states that are in the database
         */
        addStates: async (): Promise<void> => {
            const states = await this.storage.states.index();
            this.items.add(...states);
        },

        /**
         * checks if the items publicKey is not already in the database
         */
        isUnknown: (item: State): boolean => {
            return this.items.all().every((i) => i.publicKey !== item.publicKey);
        }
    }

}