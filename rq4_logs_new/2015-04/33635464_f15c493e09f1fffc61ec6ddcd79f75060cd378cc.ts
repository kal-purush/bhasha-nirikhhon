///<reference path='../defs/socket.io-client/socket.io-client.d.ts' />

module Keys {
	
	import Socket = SocketIOClient.Socket;
	
	export var LEFT = 0;
	export var RIGHT = 1;
	export var UP = 2;
	export var DOWN = 3;
	export var INVENTORY = 4;
	export var JUMP = 5;
	export var FIRE = 6;
	
	var controls = {
		37: LEFT,      // Left
		39: RIGHT,     // Right
		38: UP,        // Up
		40: DOWN,      // Down
		9:  INVENTORY, // Tab
		90: JUMP,      // Z
		88: FIRE       // X
	};
	
	var keyStates:any = {};
	
	// which keys were pressed or released on the given tick.
	export class Tick {
		ups: number[];
		downs: number[];
		
		constructor(ups:number[], downs:number[]) {
			this.ups = ups;
			this.downs = downs;
		}
		keyUp(k:number):boolean {
			return this.ups.indexOf(k) != -1;
		}
		keyHeld(k:number):boolean {
			return keyStates[k] == true;
		}
		keyDown(k:number):boolean {
			return this.downs.indexOf(k) != -1;
		}
	}
	
	var ticks:Tick[] = [];
	
	export function hasTick():boolean {
		return ticks.length !== 0;
	}
	export function popTick():Tick {
		var tick = ticks.shift();
		var i;
		for (i=0; i<tick.downs.length; i++) keyStates[tick.downs[i]] = true;
		for (i=0; i<tick.ups.length; i++) keyStates[tick.ups[i]] = false;
		return tick;
	}
	
	
	// to prevent onkeydown from being triggered more than once when the user holds the key
	var localKeyStates:any = {};
	
	export function register(socket:Socket) {
		document.onkeydown = function (e:KeyboardEvent) {
			var key = controls[e.keyCode];
			if (key != null && !localKeyStates[key]) {
				localKeyStates[key] = true;
				socket.emit('key_down', key);
			}
		};
		document.onkeyup = function (e:KeyboardEvent) {
			var key = controls[e.keyCode];
			if (key != null) {
				localKeyStates[key] = false;
				socket.emit('key_up', key);
			}
		};
		
		socket.on('key_buffer', function (buffer:any) {
			for (var i=0; i<buffer.length; i++) {
				var tickData = buffer[i];
				var ups = tickData[0];
				var downs = tickData[1];
				ticks.push(new Tick(ups, downs));
			}
		});
	}
	
}