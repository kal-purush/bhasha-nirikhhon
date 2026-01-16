import axios, { AxiosResponse } from 'axios';

// Interfaces
import IPlayer from '../common/interfaces/IPlayer';

// Utils
import baseApiUrl from '../common/utils/urlUtil';

class PlayerService {
    /**
     * Gets all players data.
     * @returns All players, or error.
     */
    public async getPlayers(): Promise<Array<IPlayer>> {
        try {
            const playersResp: AxiosResponse = await axios.get(`${baseApiUrl}/players`);
            return playersResp.data;
        } catch (e: any) {
            return e;
        }
    }
};

export default new PlayerService();