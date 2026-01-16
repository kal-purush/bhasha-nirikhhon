/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/
'use strict';

import {
  ISignal
} from 'phosphor-signaling';


/**
 * The interface required for the phosphor implementation 
 * of the Command Pattern, as described here:
 * https://en.wikipedia.org/wiki/Command_pattern
 *
 */
interface ICommand {
  /**
   * A read-only unique identifier for this Command.
   *
   * `Namespaces` can be used with dot separators, eg.
   * 'global.document.copy'.
   */
  id: string;

  /**
   * Called to invoke the command.
   */
  execute(args?: any): void;

  /**
   * A flag to determine whether this command is disabled
   * or not.
   */
  disabled: boolean;

  /**
   * A signal fired when the disabled flag changes.
   *
   */
  disabledChanged: ISignal<ICommand, boolean>;

  /**
   * A longer, more descriptive string.
   */
  caption: string;

}