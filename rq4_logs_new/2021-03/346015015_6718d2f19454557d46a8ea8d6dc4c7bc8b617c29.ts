import * as path from 'path';
import { Command } from 'commander';
import { config } from 'dotenv';
import { pipe } from 'fp-ts/lib/function';
import { getOrElse, tryCatch } from 'fp-ts/lib/Either';

import { createDirectory, createJsonFile, exitProcessError, ifElse, throwError } from './utils';
import {
  SCRIPT_OPTION,
  DEFAULT_CLAPS_LOGIN_FILE_PATH,
  DEFAULT_CLAPS_SETTING_FILE_PATH,
  INITIAL_CLASP_LOGIN_INFO,
  INITIAL_CLASP_SETTING,
  CLASP_LOGIN_FILE_NAME,
  CLAPS_SETTING_FILE_NAME,
  OUTPUT_DIRECTORY_PATH,
} from './constants';

const getOptions = () =>
  new Command()
    .option(
      `--${SCRIPT_OPTION.cd}`, //
      'execute for CD',
      false,
    )
    .option(
      `-rcp, --${SCRIPT_OPTION.clasprcPath} [filepath]`,
      'import path ".clasprc.json"',
      DEFAULT_CLAPS_LOGIN_FILE_PATH,
    )
    .option(
      `-p, --${SCRIPT_OPTION.claspPath} [filepath]`, //
      'import path ".clasp.json"',
      DEFAULT_CLAPS_SETTING_FILE_PATH,
    )
    .parse(process.argv)
    .opts();

const main = () =>
  tryCatch(() => {
    const options = getOptions();

    const isCD: boolean = options[SCRIPT_OPTION.cd];
    const claspFilePath: string = ifElse(
      () => isCD,
      () => options[SCRIPT_OPTION.claspPath],
      () => path.join(OUTPUT_DIRECTORY_PATH, CLAPS_SETTING_FILE_NAME),
    )();
    const clasprcFilePath: string = ifElse(
      () => isCD,
      () => options[SCRIPT_OPTION.clasprcPath],
      () => path.join(OUTPUT_DIRECTORY_PATH, CLASP_LOGIN_FILE_NAME),
    )();

    // eslint-disable-next-line no-console
    console.log({ isCD, claspFilePath, clasprcFilePath });

    if (!isCD) {
      config();
      createDirectory(OUTPUT_DIRECTORY_PATH);
    }

    const setting = {
      ...INITIAL_CLASP_SETTING,
      scriptId: process.env.SCRIPT_ID,
    };
    const info = {
      ...INITIAL_CLASP_LOGIN_INFO,
      token: {
        ...INITIAL_CLASP_LOGIN_INFO.token,
        refresh_token: process.env.REFRESH_TOKEN,
      },
    };

    pipe(createJsonFile(claspFilePath, setting), getOrElse<Error, void>(throwError));
    pipe(createJsonFile(clasprcFilePath, info), getOrElse<Error, void>(throwError));

    process.exit(0);
  }, exitProcessError);

main();