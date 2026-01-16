/// <reference path="./node_modules/other-module/index.d.ts" />
/// <reference path="./node_modules/some-other-module/index.d.ts" />

import * as Other from 'other-module';
import * as SomeOther from 'some-other-module';

Other.something();
SomeOther.something();