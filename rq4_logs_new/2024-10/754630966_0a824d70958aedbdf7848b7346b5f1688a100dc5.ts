/* Import Dependencies */
import axios from 'axios';

/* Import Types */
import { Dict } from 'app/Types';


/**
 * Function that searches for and fetches ROR data from the ROR API by providing a query
 * @param query The provided query to search by
 * @returns
 */
const GetRORsByName = async ({ query }: { query?: string }) => {
    let rors: Dict[] = [];

    if (query) {
        try {
            const result = await axios({
                baseURL: 'https://api.ror.org/v2',
                method: 'get',
                url: '/organizations',
                params: {
                    query
                },
                responseType: 'json'
            });

            /* Get result data from JSON */
            const data: any = result.data;

            rors = data.items;
        } catch (error) {
            console.error(error);

            throw (error);
        }
    };

    return rors;
}

export default GetRORsByName;