import * as fs from 'fs';
import * as events from 'events';
import * as child_process from 'child_process';
import debug = require('debug');

let log = debug('remoteControl:Mouse');

const READ_INTERVAL = 43;

export class Mouse extends events.EventEmitter {
	device: string;
	watcher: fs.FSWatcher;
	buf: Buffer;
	fileDescriptor: number;
	inter: NodeJS.Timer;
	constructor();
	constructor(mouseId: string);
	constructor(mouseId: number)
	constructor(mouseId?: any){
		super();
		let self = this;
		switch (typeof mouseId){
			case 'string':
				this.device = mouseId;
				break;
			case 'number':
				this.device = 'mouse' + mouseId;
				break;
			case 'undefined':
			case 'null':
			default:
				this.device = 'mice';
				break;
		}
		log('Creation, device %s', this.device);
		this.buf = new Buffer(3);
		
		fs.open('/dev/input/' + this.device, 'r', (err: NodeJS.ErrnoException, fileDescriptor: number) => {
			log('Device file opened, file descriptor:  %n', fileDescriptor);
			this.fileDescriptor = fileDescriptor;		
		});
		
		process.on('exit', function(code) {
			log('Listener destruction');
			if (self.fileDescriptor){
				fs.closeSync(self.fileDescriptor);
			}
		});
	}
	
	start() {
		
		this.inter = setInterval(() => {
			if (this.fileDescriptor){
				fs.read(this.fileDescriptor, this.buf, 0, 3, null, (err: NodeJS.ErrnoException, bytesRead: number, buffer: Buffer) => {
				
					let event = parseMouseEvent(this.buf);
					event.device = this.device;
					this.emit(MouseEventType.name(event.type), event);
					
					log('Event emitted, type: %s, data: %o', MouseEventType.name(event.type), event);
					
				});
			}else{
				log('Cleaning interval');
				clearInterval(this.inter);
			}
		}, READ_INTERVAL);
		
	}
	
	stop() {
		if (this.fileDescriptor !== undefined){
			fs.close(this.fileDescriptor);
		}
	}
}

export enum MouseEventType{
	Button,
	Moved
}

export module MouseEventType{
	export function name(type: MouseEventType): string {
		return MouseEventType[type];
	}
}


export class MouseEvent {
	leftButton: boolean;
	rightButton: boolean;
	middleButton: boolean;
	xSign: boolean;
	ySign: boolean;
	xOverflow: boolean;
	yOverflow: boolean;
	xDelta: number;
	yDelta: number;
	device: string;
	type: MouseEventType;
}

/**
 * Parse PS/2 mouse protocol
 * According to http://www.computer-engineering.org/ps2mouse/
 */
function parseMouseEvent(buffer: Buffer) : MouseEvent{
	log('Parsing mouse event');
	let event = new MouseEvent();
	
	event.leftButton = (buffer[0] & 1  ) > 0;	// Bit 0
	event.rightButton = (buffer[0] & 2  ) > 0;	// Bit 1
	event.middleButton = (buffer[0] & 4  ) > 0;	// Bit 2
	event.xSign = (buffer[0] & 16 ) > 0;		// Bit 4
	event.ySign = (buffer[0] & 32 ) > 0;		// Bit 5
	event.xOverflow = (buffer[0] & 64 ) > 0;	// Bit 6
	event.yOverflow = (buffer[0] & 128) > 0;	// Bit 7
	event.xDelta = buffer.readInt8(1);			// Byte 2 as signed int
	event.yDelta = buffer.readInt8(2);			// Byte 3 as signed int
	
	if (event.leftButton || event.rightButton || event.middleButton) {
		event.type = MouseEventType.Button;
	} else {
		event.type = MouseEventType.Moved;
	}
	return event;
}