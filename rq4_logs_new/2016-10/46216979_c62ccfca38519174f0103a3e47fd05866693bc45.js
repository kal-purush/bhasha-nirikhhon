var Main = (function (window) {
	function Main() {}

	Main.prototype = {
		convertTo: function (value, currency, places) {
			if (!places) places = 2;
			
			var ans = 1;
			switch (currency.toLowerCase()) {
				case 'inr':
					places = 3;
					ans = value * 66;
					break;

				case 'pkr':
				    places = 3;
				    ans = value * 104;
				    break;

				case 'aud':
				    places = 3;
				    ans = (value * 1.3);
				    break;

				case 'eur':
				    places = 3;
				    ans = (value * 0.9);
				    break;

				case 'gbp':
				    places = 3;
				    ans = (value * 0.8);
				    break;

			}
			var final = Math.round(ans * Math.pow(10, places)) / Math.pow(10, places);
			return final;
		}
	};

	return new Main;
}(window));