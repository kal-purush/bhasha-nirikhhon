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

package com.google.sps.servlets;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.services.tasks.model.Task;
import com.google.api.services.tasks.model.TaskList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.sps.model.AuthenticatedHttpServlet;
import com.google.sps.model.AuthenticationVerifier;
import com.google.sps.model.TasksClient;
import com.google.sps.model.TasksClientFactory;
import com.google.sps.model.TasksClientImpl;
import com.google.sps.utility.JsonUtility;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Servlet to handle posting a new tasklist */
@WebServlet("/taskLists")
public class TaskListServlet extends AuthenticatedHttpServlet {
  private final TasksClientFactory tasksClientFactory;

  /** Create servlet with default TasksClientFactory and Authentication Verifier implementations */
  public TaskListServlet() {
    tasksClientFactory = new TasksClientImpl.Factory();
  }

  /**
   * Create servlet with explicit implementations of TasksClientFactory and AuthenticationVerifier
   *
   * @param authenticationVerifier implementation of AuthenticationVerifier
   * @param tasksClientFactory implementation of GmailClientFactory
   */
  public TaskListServlet(
      AuthenticationVerifier authenticationVerifier, TasksClientFactory tasksClientFactory) {
    super(authenticationVerifier);
    this.tasksClientFactory = tasksClientFactory;
  }

  /**
   * Request to get all taskLists with their associated tasks
   *
   * @param request HTTP request from client.
   * @param response Http response to be sent to client. Contains a list of taskLists as well as a
   *     list of tasks mapped to their parent taskList id.
   * @param googleCredential valid, verified google credential object
   * @throws IOException if there's an issue with the tasks service
   */
  @Override
  public void doGet(
      HttpServletRequest request, HttpServletResponse response, Credential googleCredential)
      throws IOException {
    assert googleCredential != null
        : "Null credentials (i.e. unauthenticated requests) should already be handled";

    TasksClient tasksClient = tasksClientFactory.getTasksClient(googleCredential);

    List<TaskList> taskLists = tasksClient.listTaskLists();
    Map<String, List<Task>> taskListsWithTasks = mapTaskListsToTasks(taskLists, tasksClient);

    // Cannot use JsonUtility since gson needs to know the object is a JsonObject, not just a
    // generic object.
    Gson gson = new Gson();
    JsonObject jsonObject = new JsonObject();
    jsonObject.add("taskLists", gson.toJsonTree(taskLists));
    jsonObject.add("tasks", gson.toJsonTree(taskListsWithTasks));
    String json = gson.toJson(jsonObject);

    response.setContentType("application/json");
    response.getWriter().println(json);
  }

  /**
   * Map taskLists to the tasks they contain
   *
   * @param taskLists list of TaskList objects
   * @param tasksClient TasksClient implementation with valid Google credential
   * @return Map where keys are taskList ids and the values are all the tasks those taskLists
   *     contain
   * @throws IOException if there is an issue with the TasksService
   */
  private Map<String, List<Task>> mapTaskListsToTasks(
      List<TaskList> taskLists, TasksClient tasksClient) throws IOException {
    Map<String, List<Task>> taskListsWithTasks = new HashMap<>();
    for (TaskList taskList : taskLists) {
      taskListsWithTasks.put(taskList.getId(), tasksClient.listTasks(taskList));
    }

    return taskListsWithTasks;
  }

  /**
   * Request to create a new taskList in the user's Google Tasks account
   *
   * @param request HTTP request from client. Must contain a taskListName
   * @param response Http response to be sent to client. Will contain new TaskList object
   * @param googleCredential valid, verified google credential object
   * @throws IOException if there is a read/write issue while processing the request
   * @throws ServletException if there are unexpected issues while processing the request
   */
  @Override
  public void doPost(
      HttpServletRequest request, HttpServletResponse response, Credential googleCredential)
      throws IOException {
    assert googleCredential != null
        : "Null credentials (i.e. unauthenticated requests) should already be handled";

    TasksClient tasksClient = tasksClientFactory.getTasksClient(googleCredential);
    String taskListName = request.getParameter("taskListTitle");

    if (taskListName == null || taskListName.equals("")) {
      response.sendError(400, "taskListTitle must be present and non-empty in request");
      return;
    }

    TaskList taskList = tasksClient.postTaskList(taskListName);

    JsonUtility.sendJson(response, taskList);
  }
}