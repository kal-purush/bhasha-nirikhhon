
export function fixedTimeStepScheduler(
	{
		gameTimeUnitsPerSecond = 1,
		gameTimeUnitsPerIteration = 1
	},
	timeIterator: Iterator<number>
) {
	const secondsPerIteration = gameTimeUnitsPerIteration / gameTimeUnitsPerSecond;
	const gameTimeUnitsPerMs = gameTimeUnitsPerSecond / 1000;
	const msPerIteration = secondsPerIteration * 1000;

	var lastGameTime: number | null = null;

	const iterate = () => {
		while ( true ) {
			const lastRealTime = Date.now();
			const gameTime = timeIterator.next().value;

			if ( lastGameTime !== null && gameTime > lastGameTime ) {
				const deltaGameTime = gameTime - lastGameTime;
				lastGameTime = gameTime;

				const realTime = Date.now();
				const deltaRealTime = realTime - lastRealTime;

				const requiredDeltaRealTime = deltaGameTime / gameTimeUnitsPerMs;
				const timeToWait = requiredDeltaRealTime - deltaRealTime;

				if ( timeToWait > 0 ) {
					console.log('waiting for %d (dgt=%d, drt=%d)', timeToWait, deltaGameTime, deltaRealTime);
					return setTimeout(iterate, timeToWait);
				}
			} else {
				lastGameTime = gameTime;
			}
		}
	}

	iterate();
}