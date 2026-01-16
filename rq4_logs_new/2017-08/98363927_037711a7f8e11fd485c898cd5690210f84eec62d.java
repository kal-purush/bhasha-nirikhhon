/* Copyright 2017 Bennett Dong. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package club.cookbean.sparrow.function;

import java.util.Set;

/**
 * Created by Bennett Dong <br>
 * Date : 2017/8/7 <br>
 * Mail: dongshujin.beans@gmail.com <br> <br>
 * Desc:
 */
public interface AddFunction<Key, Value> {

    Set<Value> apply(Key key);
}