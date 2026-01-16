module application {
	export class Routes {
		
		public static HOME: string = '/';
		public static ACCOUNTS: string = '/accounts';
		public static PROFILE_DETAIL: string = '/accounts/:accountId/properties/:propertyId/profiles/:profileId';
		public static NEW_SETTINGS: string = '/new-settings';
		public static TEST: string = '/test';
	}
}