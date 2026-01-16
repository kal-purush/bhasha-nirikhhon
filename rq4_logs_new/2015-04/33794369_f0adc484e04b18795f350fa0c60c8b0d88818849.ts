/// <reference path="references.ts" />

module xjs {

	'use strict';

	export class InstagramBackgroundCtrl {

		public static $inject = ['$element', '$http', '$timeout'];

		public static USER_ID_PLACEHOLDER = '##USER-ID##';
		public static CLIENT_ID_PLACEHOLDER = '##CLIENT-ID##';

		public static FETCH_IMAGES_URL = 'https://api.instagram.com/v1/users/##USER-ID##/media/recent/?client_id=##CLIENT-ID##&callback=JSON_CALLBACK';

		private clientId: string;

		private userId: string;

		public constructor(
			private $element: ng.IAugmentedJQuery,
			private $http: ng.IHttpService,
			private $timeout: ng.ITimeoutService) {

			this.clientId = $element.attr('client-id');
			this.userId = $element.attr('user-id');
			this.init();
		}

		public init(): void {
			this.$element.css('background-color', 'red');

			var url = InstagramBackgroundCtrl.FETCH_IMAGES_URL;
			url = url.replace(InstagramBackgroundCtrl.CLIENT_ID_PLACEHOLDER, this.clientId);
			url = url.replace(InstagramBackgroundCtrl.USER_ID_PLACEHOLDER, this.userId);

			var promise = this.$http.jsonp(url, null);
			promise.success((response: any) => {
				var url: string,
					index: number;

				index = this.random(0, response.data.length);

				url = response.data[index].images.standard_resolution.url;
				this.$element.css('background-image', 'url(' + url +')');
			});
		}

		public random(min: number, max: number): number {
			return Math.floor(Math.random() * max) + min;
		}



	}

}