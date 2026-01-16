export interface IApiConfig {
	dbUrl: string;
}

export const apiConfig: IApiConfig = {
	dbUrl: 'mongodb://127.0.0.1:27017/yes',
};