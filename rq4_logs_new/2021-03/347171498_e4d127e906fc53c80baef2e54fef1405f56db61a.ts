import 'reflect-metadata';
import { ApolloServer } from 'apollo-server-express';
import express from 'express';
import { buildSchema } from 'type-graphql';
import { HelloResolver } from './hello.resolver';

export class Server {
	private readonly app = express();
	private readonly DEFAULT_PORT = parseInt(process.env.PORT) || 4000;

	constructor() {
		this.run();
	}

	private async run() {
		const schema = await buildSchema({
			resolvers: [HelloResolver],
			validate: false,
		});

		const server = new ApolloServer({
			context: ({ req, res }) => ({
				req,
				res,
			}),
			schema,
		});

		server.applyMiddleware({ app: this.app });
	}

	public listen(callback: (port: number) => void): void {
		this.app.listen(this.DEFAULT_PORT, () => {
			callback(this.DEFAULT_PORT);
		});
	}
}