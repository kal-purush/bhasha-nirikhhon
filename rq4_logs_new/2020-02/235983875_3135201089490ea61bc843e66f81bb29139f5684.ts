import { clients } from './http/htt-quack';

interface jokeResponse {
	id: string;
	joke: string;
	status: 200
}

const mapJokeResponse = (response: jokeResponse): string => {
	return response.joke;
}

/**
 * Asynkron operasjon som returerer et sitat til konsument.
 * Kilden for sitatet er tilfeldig endepunkt og returner et sitat.
 */
 // TODO: Denne utvides med flere endepunkt. Hvor ett velges tilfeldig, kalles, og mappes.
const getQuote = async (): Promise<string> => {
	const response = await clients.jokes.get<jokeResponse>('');
	return mapJokeResponse(response.data);
}

export { getQuote };