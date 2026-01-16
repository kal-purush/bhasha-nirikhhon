import { z } from 'zod';
import { config } from 'dotenv';
config();
export const env = {
	POSTGRES_DB: process.env.POSTGRES_DB,
	POSTGRES_USER: process.env.POSTGRES_USER,
	POSTGRES_PASSWORD: process.env.POSTGRES_PASSWORD,
	POSTGRES_PORT: +(process.env.POSTGRES_PORT || 5432),
	DB_HOST: process.env.DB_HOST,
	APP_PORT: +(process.env.APP_PORT || 3000),
	NODE_ENV: process.env.NODE_ENV,
	EMAIL_HOST: process.env.EMAIL_HOST,
	EMAIL_PASS: process.env.EMAIL_PASS,
	EMAIL_USER: process.env.EMAIL_USER,
	EMAIL_PORT: +(process.env.EMAIL_PORT || 587),
} as const;

export enum APP_ENVIRONVENT {
	DEVELOPMENT = 'development',
	PRODUCTION = 'production',
}

enum SMTP_PORTS {
	STANDARD = 25,
	DEFAULT = 587,
	TLS = 465,
}
const envSchema = z
	.object({
		POSTGRES_DB: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		POSTGRES_USER: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		POSTGRES_PASSWORD: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		POSTGRES_PORT: z.number().gt(0, { message: 'Port cannot be empty' }),
		DB_HOST: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		APP_PORT: z.number().gt(0, { message: 'Port cannot be empty' }),
		NODE_ENV: z.nativeEnum(APP_ENVIRONVENT),
		EMAIL_HOST: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		EMAIL_PORT: z.nativeEnum(SMTP_PORTS),
		EMAIL_USER: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
		EMAIL_PASS: z.string().min(2, { message: 'Must be atleast 2 characters long' }),
	})
	.required();

type EnvSchema = z.infer<typeof envSchema>;

export const validate = (): EnvSchema => {
	const value = envSchema.safeParse(env);
	if (!value.success) {
		throw new Error(`---${value.error.issues[0].path} :: ${value.error.issues[0].message.toUpperCase()}---`);
	}
	return value.data;
};