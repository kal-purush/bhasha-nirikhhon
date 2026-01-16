import { gameSlice } from '@/store/game/gameSlice';
import { GameStatus } from '@/store/game/gameStatus';
import store from '@/store';
import { Timer } from '@/core/game/timer';

export const keyboardEventsName = {
	up: 'ArrowUp',
	down: 'ArrowDown',
	left: 'ArrowLeft',
	right: 'ArrowRight',
};
export const gamepadMap = [
	{ index:12, event: keyboardEventsName.up },
	{ index:14, event: keyboardEventsName.left },
	{ index:15, event: keyboardEventsName.right },
	{ index:13, event: keyboardEventsName.down },
	{ index:9, event:'Escape' },
];

export class Gamepad {
	private interval?: Timer;

	init() {
		this.stop();
		window.addEventListener('gamepadconnected', this.handleGamepadEvents);
		window.addEventListener('gamepaddisconnected', this.dispatchGamePause);
	}

	private dispatchGamePause() {
		console.log('Waiting for gamepad.');
		store.dispatch(gameSlice.actions.setStatus(GameStatus.PAUSE));
	}

	private handleGamepadEvents = () => {
		this.clearInterval();
		this.interval = setInterval(this.pollGamepads, 100);
	};

	private pollGamepads = () => {
		const gamepads = navigator.getGamepads();
		if (gamepads[0]) {
			const buttons = gamepads[0].buttons;
			const leftStick = gamepads[0].axes.slice(0, 2);
			const rightStick = gamepads[0].axes.slice(-2);
			const buttonPressedIndex = buttons.findIndex((button) => button.pressed);

			this.mapButtons(buttonPressedIndex);
			this.mapStick(leftStick);
		} else {
			console.log('No gamepads connected.');
			this.clearInterval();
		}
	};

	private mapButtons = (button: number) => {
		gamepadMap.forEach((item) => {
			if (item.index === button) {
				this.dispatchKeyboardEvent(item.event);
			}
		});
	};
	private mapStick = (stick: Array<number>) => {
		let key;
		const value = 0.4;
		const normalizedStick = stick.map((axes) => {
			if (Math.abs(axes) < value) {
				return 0;
			}

			return axes;
		});
		if (normalizedStick[0] > 0) {
			key = keyboardEventsName.right;
		}
		if (normalizedStick[0] < 0) {
			key = keyboardEventsName.left;
		}
		if (normalizedStick[1] > 0)	{
			key = keyboardEventsName.down;
		}
		if (normalizedStick[1] < 0) {
			key = keyboardEventsName.up;
		}

		if (key) {
			this.dispatchKeyboardEvent(key);
		}
	};
	private dispatchKeyboardEvent = (event: string) => {
		const ketDownEvent = new KeyboardEvent('keydown', {
			code: event,
			key: event,
			view: window,
		});
		window.dispatchEvent(ketDownEvent);
	};

	stop = () => {
		window.removeEventListener('gamepadconnected', this.handleGamepadEvents);
		window.removeEventListener('gamepaddisconnected', this.dispatchGamePause);
		//Не очищаю интервал, тк событие gamepadConnected вызывается только при первом нажатии на кнопку на геймпаде
		//или при подключении геймпада к пк. Что бы геймпад заработал, приходится перезагружать страницу
	};

	private clearInterval = () => {
		if (this.interval) {
			clearInterval(this.interval as number);
		}
	};
}

export default Gamepad;