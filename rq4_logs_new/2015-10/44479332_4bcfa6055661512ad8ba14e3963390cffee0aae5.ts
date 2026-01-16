/// <reference path="typings/types.d.ts" />

import security from './modules/security/security';
import utils from './modules/utils/utils';

angular.module('fds.core', [security.name, utils.name]);