/*
 * DT-Tool
 * Copyright (c) 2024-present Carsten Rambow
 * mailto:developer AT elomagic DOT de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.elomagic.dttool;

public interface OptionsParams {

    String VERSION_MATCH = "--versionMatch";
    String VERSION_MATCH_SHORT = "-vm";

    String BATCH_MODE = "--batchMode";
    String BATCH_MODE_SHORT = "-b";

    String DEBUG = "--debug";
    String DEBUG_SHORT = "-d";

    String VERBOSE = "--verbose";
    String VERBOSE_SHORT = "-v";

}