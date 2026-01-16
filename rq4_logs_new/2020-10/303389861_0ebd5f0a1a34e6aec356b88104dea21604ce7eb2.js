import { ApolloClient, InMemoryCache, createHttpLink } from '@apollo/client';
import { setContext } from '@apollo/client/link/context';

const httpLink = createHttpLink({
	uri: '/graphql/',
	credentials: 'same-origin',
});

const csrfLink = setContext((_, { headers }) => {
	return {
		headers: {
			...headers,
			'X-CSRFToken': csrf_token,
		},
	};
});

const client = new ApolloClient({
	cache: new InMemoryCache(),
	link: csrfLink.concat(httpLink),
});

export default client;