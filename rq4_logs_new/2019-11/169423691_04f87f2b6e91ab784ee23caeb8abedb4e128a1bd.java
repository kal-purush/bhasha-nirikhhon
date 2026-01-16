/*
 * Copyright 2019 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db.fieldmask;

import com.google.protobuf.MessageOrBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class Indexes<T extends MessageOrBuilder> {
    Map<String, List<Index>> indexes = new HashMap<>();

    protected void addIndex(String indexName, String... path) {
        for (String p : path) {
            indexes.computeIfAbsent(p, v -> new ArrayList<>()).add(new Index(indexName, path));
        }
    }

    public Index getBestIndex(ObjectOrMask<T> maskedObject) {
        Index candidate = null;
        for (PathElem pe : maskedObject.getPaths()) {
            List<Index> idxList = indexes.get(pe.fullName);
            if (idxList == null) {
                continue;
            }
            int matchedElements = 0;
            for (Index idx : idxList) {
                if (matchedElements < idx.path.length) {
                    int m = 0;
                    for (String p : idx.path) {
                        if (maskedObject.getPathDef(p) != null) {
                            m++;
                        }
                    }
                    if (m > matchedElements) {
                        matchedElements = m;
                        candidate = idx;
                    }
                }
            }
            if (candidate != null) {
                break;
            }
        }
        return candidate;
    }

    public class Index {
        final String IndexName;
        final String[] path;

        public Index(String indexName, String... path) {
            IndexName = indexName;
            this.path = path;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", Indexes.class.getSimpleName() + "[", "]")
                    .add("IndexName='" + IndexName + "'")
                    .add("path=" + Arrays.toString(path))
                    .toString();
        }
    }
}