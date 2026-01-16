import { defineConfig } from "cypress";
import dotenv from 'dotenv';

dotenv.config()

export default defineConfig({
	env: {
		auth_url: 'dev-giiyewrjrwuzuc8z.us.auth0.com',
		base_url: 'localhost:3000'
	},
	e2e: {
		setupNodeEvents(on, config) {
			// implement node event listeners here
		},
	},
	chromeWebSecurity: false
});