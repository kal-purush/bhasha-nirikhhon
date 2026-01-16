/*
 * Copyright 2017 Cloudera, Inc.
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

package org.kitesdk.data;

/**
 * A callback interface to listen for updates to a partition.
 */
public interface PartitionUpdateListener {

  /**
   * A callback which is called when a partition is updated
   * (i.e. data is added or deleted)
   * @param partition the partition path that was updated
   *                  (e.g. year=2017/month=04/day=07)
   */
  void partitionUpdated(String partition);
}