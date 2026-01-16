const sleep = (ms) =>
	new Promise((resolve, reject) => setTimeout(() => resolve(), ms));

const playerElem = document.querySelector("#audioElem");

const numVoice = [
	{
		path: "./res/voice/0.wav",
		duration: 1100,
	},
	{
		path: "./res/voice/1.wav",
		duration: 1200,
	},
	{
		path: "./res/voice/2.wav",
		duration: 1100,
	},
	{
		path: "./res/voice/3.wav",
		duration: 1100,
	},
	{
		path: "./res/voice/4.wav",
		duration: 1000,
	},
	{
		path: "./res/voice/5.wav",
		duration: 1300,
	},
	{
		path: "./res/voice/6.wav",
		duration: 1200,
	},
	{
		path: "./res/voice/7.wav",
		duration: 1200,
	},
	{
		path: "./res/voice/8.wav",
		duration: 1400,
	},
	{
		path: "./res/voice/9.wav",
		duration: 1700,
	},
];

let talking = false;
const num2Speech = async (num) => {
	return new Promise(async (resolve, reject) => {
		const nums = num.toString().split("");

		if (nums.length > 0) {
			const voice = numVoice[parseInt(nums.shift())];

			playerElem.src = voice.path;
			playerElem.load();
			playerElem.play();

			await sleep(voice.duration);
			await num2Speech(nums.join(""));
		}

		resolve();
	});
};