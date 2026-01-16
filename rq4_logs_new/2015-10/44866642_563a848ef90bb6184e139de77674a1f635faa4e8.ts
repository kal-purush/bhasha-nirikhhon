/// <reference path="GameMap.ts" />

var tones = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B", "+C"];

// HELPERS
var currentTime = 0;
var currentQ: { settings: BubbleSettings; timeStamp: number }[];

function reset()
{
	currentTime = 0;
	currentQ = [];
}
function addTone(symbol: string, duration: number = 1, progress: boolean = true)
{
	duration *= 300;
	currentQ.push({ settings: { frequency: tones.indexOf(symbol) / 12, life: duration }, timeStamp: currentTime });
	if (progress)
		currentTime += duration;
}

function mapCalibrate(): GameMap
{
	reset();
	
	addTone("C", 8, false);
	addTone("+C", 8);
	currentTime += 5000;
	tones.forEach(t => addTone(t));
	
	return { name: "Calibrate", queue: currentQ };
}

function mapEntchen(): GameMap
{
	reset();
		
	addTone("C");
	addTone("D");
	addTone("E");
	addTone("F");
	addTone("G", 2);
	addTone("G", 2);
	addTone("A");
	addTone("A");
	addTone("A");
	addTone("A");
	addTone("G", 4);
	addTone("A");
	addTone("A");
	addTone("A");
	addTone("A");
	addTone("G", 4);
	addTone("F");
	addTone("F");
	addTone("F");
	addTone("F");
	addTone("E", 2);
	addTone("E", 2);
	addTone("D");
	addTone("D");
	addTone("D");
	addTone("D");
	addTone("C", 4);
	
	return { name: "Entchen", queue: currentQ };
}

function mapMario(): GameMap
{
	reset();
	
	addTone("A", 0.5);
	addTone("A", 1);
	addTone("A", 1);
	addTone("F", 0.5);
	addTone("A", 1);
	addTone("+C", 2);
	addTone("C", 2);
		
	for (var i = 0; i < 2; i++)
	{
		addTone("F", 1.5);
		addTone("C", 1.5);
		addTone("C", 1.5); //-A
		addTone("D", 1);
		addTone("E", 1);
		addTone("D#", 0.5);
		addTone("D", 1);
		addTone("C", 0.5);
		addTone("A", 1);
		addTone("+C", 0.5);
		addTone("+C", 1); // +D
		addTone("A#", 0.5);
		addTone("C", 1);
		addTone("A", 1);
		addTone("F", 0.5);
		addTone("G", 0.5);
		addTone("E", 1.5);
	}
	
	addTone("+C", 0.5);
	addTone("B", 0.5);
	addTone("A#", 0.5);
	addTone("G", 1);
	addTone("A", 1);
	addTone("C", 0.5);
	addTone("D", 0.5);
	addTone("F", 1);
	addTone("D", 0.5);
	addTone("F", 0.5);
	addTone("G", 1);
	
	addTone("+C", 0.5);
	addTone("B", 0.5);
	addTone("A#", 0.5);
	addTone("G", 1);
	addTone("A", 1);
	addTone("+C", 1);
	addTone("+C", 0.5);
	addTone("+C", 1);
	
	addTone("+C", 0.5);
	addTone("B", 0.5);
	addTone("A#", 0.5);
	addTone("G", 1);
	addTone("A", 1);
	addTone("C", 0.5);
	addTone("D", 0.5);
	addTone("F", 1);
	addTone("D", 0.5);
	addTone("F", 0.5);
	addTone("G", 1.5);
	
	addTone("G#", 1.5);
	addTone("G", 1.5);
	addTone("F", 2);
	
	
	addTone("F", 0.5);
	addTone("F", 1);
	addTone("F", 1);
	addTone("F", 0.5);
	addTone("G", 1);
	addTone("A", 0.5);
	addTone("F", 1);
	addTone("D", 0.5);
	addTone("C", 2);
	
	addTone("F", 0.5);
	addTone("F", 1);
	addTone("F", 1);
	addTone("F", 0.5);
	addTone("G", 1);
	addTone("A", 4);
	
	addTone("F", 0.5);
	addTone("F", 1);
	addTone("F", 1);
	addTone("F", 0.5);
	addTone("G", 1);
	addTone("A", 0.5);
	addTone("F", 1);
	addTone("D", 0.5);
	addTone("C", 2);
	
	addTone("A", 0.5);
	addTone("A", 1);
	addTone("A", 1);
	addTone("F", 0.5);
	addTone("A", 1);
	addTone("+C", 2);
	addTone("C", 2);
	
	addTone("A", 8);
	
	
	return { name: "Mario", queue: currentQ };
}

function mapGodfather(): GameMap
{
	reset();
		
	addTone("C");
	addTone("F");
	addTone("G#");
	addTone("G");
	addTone("F");
	addTone("G#");
	addTone("F");
	addTone("G");
	addTone("F");
	addTone("C#");
	addTone("E");
	addTone("C", 4);
	
	return { name: "Godfather", queue: currentQ };
}

function getMaps(): GameMap[]
{
    var maps: GameMap[] = [];
	maps.push(mapCalibrate());
	maps.push(mapEntchen());
	maps.push(mapMario());
	maps.push(mapGodfather());
	return maps;
}

export = getMaps;