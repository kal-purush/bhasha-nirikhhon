// pps workspace id 43506236234351
// Operation 2020 project id: 1166129760939180
// Inventory Management section id: 1175004759431138



function Asana_getTask(taskId, accessToken)
{
  return fetchAsanaUrl("tasks/" + taskId, null);
}


// Returns new task id (as string) or null.
function Asana_createTask(title, notes, sectionId, assigneeId)
{
  Logger.log("Asana_createTask: " + title);
  const ppsWorkspaceId = "43506236234351";
  var createOptions = 
  {
    "assignee" : assigneeId,
    "name" : title,
    "notes" : notes,
    "workspace" : ppsWorkspaceId
  };
  
  var responseJson = fetchAsanaUrl("tasks", createOptions);
  if (responseJson === null)
  {
    Logger.log("Failed to create task.");
    return null;
  }
  
  if (!("data" in responseJson) || !("gid" in responseJson.data))
  {
    Logger.log("Unexpected response from task creation.");
    return null;
  }
  
  const newTaskId = responseJson.data.gid;
  
  var path = "sections/" + sectionId + "/addTask";
  
  var addToSectionInfo = { "task" : newTaskId };
        
  responseJson = fetchAsanaUrl(path, addToSectionInfo);
  if (responseJson === null)
  {
    Logger.log("Failed to add task to section.");
    return null;
  }

  Logger.log("Added new task " + newTaskId + " to section " + sectionId);

  return newTaskId;  
}



function testAsana_createTask()
{
  const sectionId = "1175004759431138"; // Inv Mana
  const assigneeId = "134017349624135"; // malc
  
  var newTaskId = Asana_createTask("Testing auto-created tasks", "This test the notes field", sectionId, assigneeId);
  Logger.log(newTaskId);
}



/**
 * Call an Asana API function.
 *
 * @param path  Defines the function to call.  This function appends path to
 *              the main Asana API url.  
 *              For example, "tasks/1234" gets info about task 1234.
 * @param info  JSON object containing function parameters.  This function 
 *              sets "info" as the value for a key called "data" in the 
 *              dictionary that it sends to Asana.  Pass null if there are
 *              no embedded parameters.
 * @param method  Optional.  Specify "PUT" to override default "POST" if info is not null.
 *
 * @return  Asana's response as a JSON object, or null in failure cases.
 */
function fetchAsanaUrl(path, info, method)
{
  // Set testingFetch true to examine the request we actually send.
  const testingFetch = false;

  var asanaApiBaseUrl = "https://app.asana.com/api/1.0/";
  var url = asanaApiBaseUrl + path;
  
  accessToken = getProperty("AsanaAccessToken");
 
  if (testingFetch)
  {
    url = "https://httpbin.org/post"
    accessToken = "hohoho";
  }
  
  var options = 
  {
    "muteHttpExceptions" : true,
    "headers" : {"Authorization": "Bearer " + accessToken}
  };
  
  if (info == null)
  {
    options.method = "GET";
  }
  else
  {
    options.method = (method === undefined) ? "POST" : method;
    options.payload = JSON.stringify({"data" : info});
    options.contentType = 'application/json';
  }
  
  Logger.log(url);
  Logger.log(options);

  var request = UrlFetchApp.getRequest(url, options);
  Logger.log(request);
  
  try
  {
    var result = UrlFetchApp.fetch(url, options);
    var responseText = result.getContentText();
    Logger.log(responseText);
    var responseJson = JSON.parse(responseText);
    if ("errors" in responseJson)
    {
      // errors is an array of dictionaries
      for (const dict of responseJson.errors)
      {
        if ("message" in dict)
        {
          Logger.log(dict.message);
          return null;
        }
      }
    }                 
    return responseJson;
  } 
  catch (e)
  {
    Logger.log("Caught exception." + e.message);
    return null;
  }
}



// Attachments
// https://app.asana.com/api/1.0/tasks/1176874649688496/attachments
// If task has no attachments, response is {"data":[]}
// Response with three attachments:
// {"data":[{"gid":"1176874649688504","name":"nk.jpg","resource_type":"attachment"},{"gid":"1176874649688507","name":"ben-more-1.jpg","resource_type":"attachment"},{"gid":"1176874649688510","name":"2020-05-21-1018.tvr","resource_type":"attachment"}]}
// Response to GET attachments/gid includes time-limited download URL.  Main fields:
// {
//    "data": {
//        "download_url": "https://asana-user-private-us-east-1.s3.amazonaws.com/assets/43506-massive-_=_",
//        "name": "nk.jpg"
//        }
// }
// https://app.asana.com/api/1.0/attachments/1176874649688504
function Asana_getTaskAttachments(taskId)
{
  var path = "tasks/" + taskId + "/attachments";
  // FIXME - get attachment gids!
  const attachments = fetchAsanaUrl(path, null);
  
  if (!("data" in attachments))
  {
    Logger.log("Failed to find 'data' in task/attachments");
    return null;
  }
  
  attachmentData = [];
  for (var attachment of attachments.data)
  {
    if (!('gid' in attachment))
    {
      Logger.log("Attachment has no gid");
      continue;
    }
    path = "attachments/" + attachment.gid;
    let responseJson = fetchAsanaUrl(path, null);
    if (responseJson === null ||
        !('data' in responseJson) ||
        !('download_url' in responseJson.data) ||
        !('name' in responseJson.data))
    {
      Logger.log("Missing or incomplete attachment info.");
      continue;
    }
    attachmentData.push(responseJson.data);
  }
  
  return attachmentData;
}



function testGetTaskAttachments()
{
  var answer = Asana_getTaskAttachments(1176874649688496);
  if (answer === null)
  {
    Logger.log("Fail");
    return;
  }
  Logger.log(JSON.stringify(answer));
}



/**
 * Get status of asynchronous job.
 *
 * @param jobId  id obtained from response to prior asnychronous call.
 *
 * @return response, or null.
 */
function Asana_getJobStatus(jobId)
{
  var path = "jobs/" + jobId;

  const response = fetchAsanaUrl(path, null);
  if (response === null)
  {
    Logger.log("Failed to get job status.");
    return null;
  }
  
  if (!("data" in response) || 
      !("status" in response.data))
  {
    Logger.log("Unexpected response when getting job status.");
    return null;
  }
  
  return response;
}



/**
 * Duplicate a project and all its tasks.
 *
 * @param template  Id (as string) of project to duplicate.  Also
 *                  accepts specific strings such as 'rma'.
 * @param name  Name for new project.
 *
 * @return  Info about async job to create the new project (typically
 *          in progress but not complete).
 */
function Asana_duplicateProject(template, name)
{
  var templateId;
  
  if (template === "rma")
  {
    templateId = "1176696000640487";
  }
  else if (template === "csp")
  {
    templateId = "1151738461865886";
  }
  else
  {
    templateId = template;
  }
  
  var path = "projects/" + templateId + "/duplicate";

  var options = 
  {
    "name" : name
  };

  const response = fetchAsanaUrl(path, options);
  if (response === null)
  {
    Logger.log("Failed to duplicate project.");
    return null;
  }
  
  if (!("data" in response) || 
      !("new_project" in response.data) ||
      !("gid" in response.data.new_project))
  {
    Logger.log("Unexpected response from project duplication.");
    return null;
  }
  
  return response;
}

/**
 * Wait for async job to complete.
 *
 * @param jobId  Job to wait for.
 * @param steps  Maximum number of status checks before timing out.
 * @param interval  Time (milliseconds) between status checks.
 *
 * @return  Most recent response.
 */
function Asana_waitForJob(jobId, steps, interval)
{
  var response;
  
  for (let i = 0; i < steps; i++)
  {
    response = Asana_getJobStatus(jobId);
    if (response.data.status !== "in_progress")
    {
      // Job is finished
      break;
    }
    Logger.log("Iteration " + i + ": " + response.data.status);
    Utilities.sleep(interval);
  }
  
  return response;
}



function testAsana_duplicateProject()
{
  const rmaTemplate = "rma"; //"1176696000640487";
  var response = Asana_duplicateProject(rmaTemplate, "RMA 912 Malcolm Test");
  if (response == null)
  {
    Logger.log("Failed");
    return;
  }
  Logger.log(response.data.new_project.gid);
  
  response = Asana_waitForJob(response.data.gid, 10, 2000);
  Logger.log("Final status: " + response.data.status);
}



/**
 * Get URL for given Tech Support task id.
 *
 * @param techSupportTask  id for task.
 *
 * @return Full URL
 */
function Asana_getSupportUrl(techSupportTask)
{
  // The following hard-coded ids avoid unneeded fetches.
  // Id for PPS organisation
  const ppsOrgId = "43506236234351";
  // Id for "Tech Sup. & RMAs" team
  const techRmaTeam = "1177436406373576";
  // Id for "Tech Support - PPS" project within above team
  const techPpsProject = "1175836475924949";
  
  return "https://app.asana.com/0/" + techPpsProject + "/" + techSupportTask + "/f";
}



function Asana_getProjectUrl(projectId)
{
  return "https://app.asana.com/0/" + projectId;
}



/**
 * Move a project to a different team.
 *
 * @param project id of project to move.
 * @param newTeam id of destination team.
 *
 * @return true if successful.
 */
function Asana_moveProject(project, newTeam)
{
  var path = "projects/" + project;

  var options = 
  {
    "team" : newTeam
  };

  const response = fetchAsanaUrl(path, options, "PUT");
  if (response === null)
  {
    Logger.log("Failed to move project.");
    return false;
  }
  
  if (!("data" in response) || 
      !("team" in response.data) ||
      !("gid" in response.data.team))
  {
    Logger.log("Unexpected response from project move.");
    return false;
  }
  
  return response.data.team.gid === newTeam;
}



/**
 * Move specified project to "Tech Support and RMA" team.
 *
 * @param project id of project to move.
 *
 * @return true if successful.
 */
function Asana_moveProjectToRma(project)
{
  // Id for "Tech Sup. & RMAs" team
  const techRmaTeam = "1177436406373576";
  
  return Asana_moveProject(project, techRmaTeam);
}



function testMoveProjectToRma()
{
  if (!Asana_moveProjectToRma("1185324938540322"))
  {
    Logger.log("Failed to move project");
    return;
  }
  
  Logger.log("Moved project");
}



/**
 * Find tasks whose names contain specified terms.
 *
 * @param project  The project owning the tasks to be searched.
 * @param searchTerms  Array of phrases to search for.  Assumes 
 *                     1:1 phrase:task mapping.
 *
 * @return Array of task ids.  Each entry may be undefined if the
 *         corresponding search term was not found.
 */
function Asana_findProjectTasks(project, searchTerms)
{
  var foundTasks = [];

  var path = "projects/" + project + "/tasks";

  const response = fetchAsanaUrl(path, null);
  if (response === null)
  {
    Logger.log("Failed to find project.");
    return foundTasks;
  }
  
  if (!("data" in response))
  {
    Logger.log("Unexpected response from project tasks.");
    return foundTasks;
  }
 
  for (const task of response.data)
  {
    let nameLower = task.name.toLowerCase();
    
    for (let i = 0; i < searchTerms.length; i++)
    {
      if (nameLower.includes(searchTerms[i]))
      {
        foundTasks[i] = task.gid;
        break;
      }
    }
  }
        
  return foundTasks;
}



function testFindTasks()
{
  const searchTerms = ["received photos", "document resolution", "notify customer"];
  
  const foundTasks = Asana_findProjectTasks("1185083383484237", searchTerms);
  
  Logger.log(foundTasks);
}



/**
 * Insert text and optional link into task description.
 *
 * @param task  Task to be modified.
 * @param items  Array of dictionaries, each having text and optional URL.
 *               Each item is added as a line in the task's description field.
 *               The line is the text, linkified if a URL is supplied.
 *
 * @return true if successful.
 */
function Asana_setTaskDescription(task, items)
{
  var path = "tasks/" + task;

  var html = '<body><ul>';
  
  // Each item is a dictionary: text and link
  for (const item of items)
  {
    html += '<li>';
    if (item.link !== undefined)
    {
      html += '<a href="' + item.link + '">';
    }
    html += item.text;
    if (item.link !== undefined)
    {
      html += '</a>';
    }
    html += '</li>';
  }

  html += '</ul></body>';
  
  var options = 
  {
    "html_notes" : html
  };

  const response = fetchAsanaUrl(path, options, "PUT");
  if (response === null)
  {
    Logger.log("Failed to set task description.");
    return false;
  }

  return true;
}



function testSetDescription()
{
  var items = [];
 
  items.push({text: "BBC", link: "http://bbc.co.uk"});
  items.push({text: "CNN", link: "http://cnn.com"});
  
  Asana_setTaskDescription(
    "1194339436689464",
    items);
}