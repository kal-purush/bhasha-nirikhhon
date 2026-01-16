
class SinGenerator {
	constructor(freq, sampleRate) {
		this.freq = freq
		this.sampleRate = sampleRate

		// Calculate step in terms of array sin table elements to reduce the
		// amount of math we will need to do on the fly.
		this._step = (this.freq * SinGenerator._sinTable.length) / this.sampleRate
		this._curPos = 0
	}

	// TODO: Generate this.
	static _sinTable = [
		0.000,  0.259,  0.500,  0.707,  0.866,  0.966,
		1.000,  0.966,  0.866,  0.707,  0.500,  0.259,
		-0.000, -0.259, -0.500, -0.707, -0.866, -0.966,
		-1.000, -0.966, -0.866, -0.707, -0.500, -0.259
	]

	// Advances the sin angle by one step, according to the frequency and 
	// sample rate.
	// Returns the interpolated value of the sin wave at the pre-step position.
	Step() {
		let prePos = this._curPos
		this._curPos += this._step
		if (this._curPos >= SinGenerator._sinTable.length) {
			this._curPos -= SinGenerator._sinTable.length
		}

		let lowerIdx = Math.floor(prePos)
		let dist = prePos - lowerIdx;
		let a = SinGenerator._sinTable[lowerIdx]
		let b = SinGenerator._sinTable[(lowerIdx + 1) % SinGenerator._sinTable.length]
		return SinGenerator._lerp(a, b, dist)
	}

	static _lerp(a, b, dist) {
		return a + ((b - a) * dist)
	}
}