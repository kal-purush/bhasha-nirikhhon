import {CWD_PATH} from '@/config';
import {resolve} from 'path';

export const ASSETS_DIR_NAME = 'assets';
export const ASSETS_PATH = resolve(CWD_PATH, `public/${ASSETS_DIR_NAME}`);