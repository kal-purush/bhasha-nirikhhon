declare function global(varName: string): any;

export function ComposePingResponse(): string {
	var latLngPattern = /([^,]+),(.+)/;
	var maxAge = 1200; //20 mins

	var bestLocation = [
		{ LatLngVar: "%LOC", TimeVar: "%LOCTMS" },
		{ LatLngVar: "%LOCN", TimeVar: "%LOCNTMS" }
	]
		.map(l => {
			var latLngMatch: RegExpMatchArray;
			if (global(l.LatLngVar) && (latLngMatch = global(l.LatLngVar).match(latLngPattern))) {
				var age = global(l.TimeVar) - global("%TIMES");
				return {
					Lat: latLngMatch[1],
					Lng: latLngMatch[2],
					Age: age
				};
			}
			else {
				return null;
			}
		})
		.filter(l => !!l)
		.filter(l => l.Age < maxAge)
		.sort((a, b) => a.Age - b.Age)
	[0];

	if (bestLocation) {
		var gmapsUrl = `http;//maps.google.com/maps?z=12&t=m&q=loc:${bestLocation.Lat}+${bestLocation.Lng}`;
		return gmapsUrl;
	}
	else {
		return "Unable to get location";
	}
}