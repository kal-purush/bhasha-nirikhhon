/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/
'use-strict';

import {
  IKeyBinding, KeymapManager
} from '../lib/index';


/**
 * A list of keyboard shortcuts for the example.
 */
var SHORTCUTS = [
  'a',
  'b',
  'c',
  'd',
  'd d',
  'ctrl+=',
  'ctrl+-',
  'ctrl+a',
  'shift+b',
  'ctrl+[',
  'ctrl+alt+0',
  'ctrl+k ctrl+;',
  'shift+F7 shift+F8',
  'alt+n alt+m shift+alt+o',
];


/**
 * A shortcut handler function which logs the sequence.
 *
 * This handler always returns true to stop event propagation.
 */
function logHandler(sequence: string): boolean {
  var span = document.getElementById('log-span');
  span.textContent = sequence;
  return true;
}


/**
 * Create a log key binding for the given key sequence.
 */
function makeLogBinding(sequence: string): IKeyBinding {
  return { sequence: sequence, handler: logHandler.bind(void 0, sequence) };
}


/**
 * Create an unordered list from an array of strings.
 */
function createList(data: string[]): HTMLElement {
  var ul = document.createElement('ul');
  ul.innerHTML = data.map(text => `<li>${text}</li>`).join('');
  return ul;
}


/**
 * The main application entry point.
 */
function main(): void {
  // Create the key bindings for the shortcuts.
  var bindings = SHORTCUTS.map(makeLogBinding);

  // Initialize the keymap manager with the bindings.
  // These bindings are applied globally ('*').
  var keymap = new KeymapManager();
  keymap.add('*', bindings);

  // Setup the keydown listener for the document.
  document.addEventListener('keydown', event => {
    keymap.processKeydownEvent(event);
  });

  // Create and add the list of shortcuts to the DOM.
  var host = document.getElementById('list-host');
  host.appendChild(createList(SHORTCUTS));
}


window.onload = main;