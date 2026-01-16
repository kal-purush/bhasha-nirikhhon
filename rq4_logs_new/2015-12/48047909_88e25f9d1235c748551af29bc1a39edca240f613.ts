/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/
'use-strict';

import {
Message
} from 'phosphor-messaging';

import {
Panel
} from 'phosphor-widget';

import {
  updateStatus
} from './status';

import './commandpalette.css';


const PALETTE_CLASS = 'p-Command-Palette';

const HEADER_CLASS = 'p-header';

const COMMAND_CLASS = 'p-command';

export
interface ICommandSpec {
  score?: number;
  originalText: string;
  command: {
    id: string;
    caption: string;
  }
}

export
interface ICommandSection { header: string, specs: ICommandSpec[] };

export
class CommandPalette extends Panel {

  get commands(): ICommandSection[] {
    return this._commands;
  }

  set commands(commands: ICommandSection[]) {
    this._commands = commands;
    this._emptyPalette();
    this._commands.forEach(section => { this._renderSection(section); });
  }

  constructor() {
    super();
    this.addClass(PALETTE_CLASS);
  }

  private _emptyPalette(): void {
    let { node } = this;
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  private _renderCommandSpec(spec: ICommandSpec): void {
    let command = document.createElement('div');
    command.classList.add(COMMAND_CLASS);
    command.appendChild(document.createTextNode(spec.command.caption));
    this.node.appendChild(command);
  }

  private _renderHeader(title: string): void {
    let header = document.createElement('div');
    header.classList.add(HEADER_CLASS);
    header.appendChild(document.createTextNode(title));
    header.appendChild(document.createElement('hr'));
    this.node.appendChild(header);
  }

  private _renderSection(section: ICommandSection): void {
    console.log('here', section.header);
    this._renderHeader(section.header);
    section.specs.forEach(spec => { this._renderCommandSpec(spec); });
  }

  private _commands: ICommandSection[] = null;
}