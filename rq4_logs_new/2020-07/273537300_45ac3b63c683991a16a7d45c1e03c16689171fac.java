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

package com.google.sps.exceptions;

/** Exception to be thrown when an issue occurs with the Google Places API */
public class PlacesException extends Exception {
  /**
   * Constructor to create a PlaceException with the given message
   *
   * @param message String to display on console when exception is thrown
   */
  public PlacesException(String message) {
    super(message);
  }

  /**
   * Constructor to create a PlaceException with the given message and cause
   *
   * @param message String to display on console when exception is thrown
   * @param cause Throwable to display on console when exception is thrown
   */
  public PlacesException(String message, Throwable cause) {
    super(message, cause);
  }
}