import express from "express";
import bodyParser from "body-parser";
import { RegisterRoutes } from "../build/routes";
import helmet from "helmet";
import compression from "compression";

const run = async () => {
	const app = express();

	try {
		// Use body parser to read sent json payloads
		app.use(
			bodyParser.urlencoded({
				extended: true,
			})
		);

		app.use(bodyParser.json());

		app.use(helmet());

		app.use(compression());

		RegisterRoutes(app);
	} catch (error) {
		console.log("Process exited with error: ", error);
		process.exit(1);
	}

	const port = process.env.PORT || 4500;

	app.listen(port, () =>
		console.log(`Example app listening at http://localhost:${port}`)
	);
};

run();