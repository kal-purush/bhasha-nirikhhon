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

package com.google.sps.model;

import java.util.List;

/**
 * Contains the summary tasks information that should be passed to the client, as well as the
 * methods to generate these statistics.
 */
public final class TasksResponse {
  private final List<String> taskListNames;
  private final int tasksToComplete;
  private final int tasksDueToday;
  private final int tasksCompletedToday;
  private final int tasksOverdue;

  /**
   * Create a TasksResponse instance
   *
   * @param taskListNames names of all task lists to display in a multiselect dropdown list on the
   *     client.
   * @param tasksToComplete number of tasks the user has yet to complete.
   * @param tasksDueToday number of tasks the user has yet to complete for today.
   * @param tasksCompletedToday number of tasks the user has marked as complete for today.
   * @param tasksOverdue number of tasks that have due dates before today.
   */
  public TasksResponse(
      List<String> taskListNames,
      int tasksToComplete,
      int tasksDueToday,
      int tasksCompletedToday,
      int tasksOverdue) {
    this.taskListNames = taskListNames;
    this.tasksToComplete = tasksToComplete;
    this.tasksDueToday = tasksDueToday;
    this.tasksCompletedToday = tasksCompletedToday;
    this.tasksOverdue = tasksOverdue;
  }

  public List<String> getTaskListNames() {
    return taskListNames;
  }

  public int getTasksToComplete() {
    return tasksToComplete;
  }

  public int getTasksDueToday() {
    return tasksDueToday;
  }

  public int getTasksCompletedToday() {
    return tasksCompletedToday;
  }

  public int getTasksOverdue() {
    return tasksOverdue;
  }

  @Override
  public boolean equals(final Object object) {
    if (object == null || object.getClass() != getClass()) {
      return false;
    }

    if (object == this) {
      return true;
    }

    TasksResponse other = (TasksResponse) object;

    return (this.taskListNames == other.getTaskListNames()
        && this.tasksToComplete == other.getTasksToComplete()
        && this.tasksDueToday == other.getTasksDueToday()
        && this.tasksCompletedToday == other.getTasksCompletedToday()
        && this.tasksOverdue == other.getTasksOverdue());
  }
}