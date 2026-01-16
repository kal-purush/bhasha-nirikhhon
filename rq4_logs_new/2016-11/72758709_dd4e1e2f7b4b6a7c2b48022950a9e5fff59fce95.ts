/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

/**
 * Adapted for TypeScript by Zalando / Team Norris
 *
 * See https://github.com/facebookincubator/create-react-app/tree/master/packages/react-dev-utils
 * for original implementation.
 */

import * as readline from 'readline';

// Convention: "no" should be the conservative choice.
// If you mistype the answer, we'll always take it as a "no".
// You can control the behavior on <Enter> with `isYesDefault`.
export default (question: string, isYesDefault: boolean): Promise<any> => {
  return new Promise(resolve => {
    let hint = (isYesDefault && '[Y/n]') || '[y/N]';
    let message = `${question} ${hint}\n`;
    let rlInterface = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    rlInterface.question(message, (answer: string) => {
      rlInterface.close();

      let useDefault = answer.trim().length === 0;
      if (useDefault) {
        return resolve(isYesDefault);
      }

      let isYes = answer.match(/^(yes|y)$/i);
      return resolve(isYes);
    });
  });
};