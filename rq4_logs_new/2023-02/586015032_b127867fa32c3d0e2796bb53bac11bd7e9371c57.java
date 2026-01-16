/*
 * Copyright 2023 Ilya Shishkov (https://github.com/shishkovilja)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ignitop.ui.keyhandler;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class KeyPressHandler {
    /** Key press actions. */
    private final Map<Character, Runnable> keyPressActions = new HashMap<>();

    /**
     * @param key Character of pressed key.
     * @param action Action.
     */
    public void addKeyHandler(char key, Runnable action) {
        keyPressActions.put(key, action);
    }

    /**
     * @param key Character of pressed key.
     */
    public void handle(char key) {
        Runnable action = keyPressActions.get(key);

        if (action != null)
            action.run();
        else
            System.out.println("Unsupported key: " + key);
    }
}