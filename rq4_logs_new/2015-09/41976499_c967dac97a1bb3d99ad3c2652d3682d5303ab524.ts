/*
	Description: Provides named constants that are used throughout the rest of the code.
*/

module constants {
	// State Machine Constants
	export const MENU_STATE: number = 0;
	export const PLAY_STATE: number = 1;
	export const GAME_OVER_STATE: number = 2;
	export const INSTRUCTIONS_STATE: number = 3;

	// Game Constants
	export const GAME_VERSION: string = "v0.4.2.1";
	export const GAME_FPS: number = 60;
	export const ENEMY_NUM: number[] = [2, 3, 4, 5];
	export const LABEL_FONT: string = "35px Consolas";
	export const INSTRUCTIONS_FONT: string = "20px Consolas";
	export const VERSION_FONT: string = "12px Consolas";
	export const LABEL_COLOUR: string = "#00FF00";
	export const PLAYER_LIVES: number = 4;
	export const SHIP_LASER_TYPE: string = "greenlaser1";
	export const SHIP_LASER_SPEED: number = 10;
	export const SHIP_LASER_SOUNDTYPE: number = 1;

	// Cursors
	export const CURSOR_DEFAULT: string = "url('assets/images/cursors/default.png'), default";
	export const CURSOR_POINTER: string = "url('assets/images/cursors/pointer.png'), pointer";
}