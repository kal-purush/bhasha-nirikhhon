// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.sps.utility;

import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.appengine.repackaged.com.google.gson.reflect.TypeToken;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Scanner;

/** Class to interact with the Secret Manager API. */
public final class KeyProvider {

  private File file;

  /** Constructor to create instance with file linked to default path. */
  public KeyProvider() {
    this.file = new File("src/main/resources/KEYS.json");
  }

  /**
   * Constructor to create instance with file linked to specified file path.
   *
   * @param file An object which contains the path to the file storing the keys.
   */
  public KeyProvider(File file) {
    this.file = file;
  }

  /**
   * Gets the value of a key from the Secret Manager API.
   *
   * @param name A string representing the name of the key.
   * @return A string representing the value of stored under the given key.
   * @throws IOException
   */
  public String getKey(String name) throws IOException {
    StringBuilder rawJson = new StringBuilder();
    try (Scanner reader = new Scanner(file)) {
      while (reader.hasNextLine()) {
        rawJson.append(reader.nextLine());
      }
    }
    Gson gson = new Gson();
    Type mapType = new TypeToken<Map<String, String>>() {}.getType();
    Map<String, String> keys = gson.fromJson(rawJson.toString(), mapType);
    return keys.get(name);
  }
}