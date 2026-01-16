/**
 * Static class to encapsulate application wide configuration settings
 */
export class AppSettings{
	/**
	 * Empty default contructor
	 */
	constructor(){}
	
	/**
	 * gets the common backend service hostname.
	 * @returns {string} base url for application services that use http
	 */
	public serviceHost(): string{
		
		//return ''; //PRODUCTION
		return 'http://localhost:62734'; //DEVELOPMENT
	}
}