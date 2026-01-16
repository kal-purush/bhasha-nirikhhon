import db.Database;
import db.Entity;
import db.exception.EntityNotFoundException;
import db.exception.InvalidEntityException;
import todo.entity.Step;
import todo.entity.Task;
import todo.service.StepService;
import todo.service.TaskService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Main {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args){
        System.out.println("*TO_DO List App*");
        System.out.println("What do you want to do?");

        while (true){
        String input = scanner.nextLine().trim().toLowerCase();
    try {

        switch (input) {
            case "add task":
                AddTask();
                break;
            case "add step":
                AddStep();
                break;
            case "delete":
                delete();
                break;
            case "update task":
                updateTask();
                break;
            case "update step":
                updateStep();
                break;
            case "get task-by-id":
                getTaskByID();
                break;
            case "get all-tasks":
                getAllTasks();
                break;
            case "get incomplete-tasks":
                getIncompleteTasks();
                break;
            case "exit":
                System.out.println("Good lock !");
                return;
            default:
                System.out.println("Please type correct command !");
        }
    } catch (Exception e){
        System.out.println("Error!...."+ e.getMessage());
    }
        }
    }

    private static void AddTask() throws InvalidEntityException , ParseException {
    System.out.println("-Title : ");
    String title = scanner.nextLine() ;

    System.out.println("-Description : ");
    String description = scanner.nextLine() ;

    System.out.println("-Due date : ");
    String dueDateStr = scanner.nextLine().trim();
    Date dueDate = dateFormat.parse(dueDateStr);

    Task task = TaskService.CreatTask(title,description,dueDate) ;
    System.out.println("*Task added successfully ! ");
    System.out.println(" Task ID : "+ task.id );
    }

    private static void AddStep() throws EntityNotFoundException , InvalidEntityException{
        System.out.println("-Task ID : ");
        int taskID = Integer.parseInt(scanner.nextLine());

        try {
            Entity entity = Database.get(taskID) ;
            if (entity == null || entity.getEntityCode() != 1){
                System.out.println("Cannot save step!");
                System.out.println("Error!..... Cannot find task with ID = "+ taskID);
                return;
            }

        System.out.println("-Title :");
        String title = scanner.nextLine().trim() ;

        if (title.isEmpty()){
            System.out.println("Cannot save step!");
            System.out.println("Error!..... Step title cannot be 'Empty'! ");
            return;
        }

            Step step = StepService.createStep(title,taskID) ;

            System.out.println("Step saved successfully.");
            System.out.println("-ID: " + step.id);
            System.out.println("Creation Date: " + new Date());

        } catch (EntityNotFoundException e) {
            System.out.println("Cannot save step.");
            System.out.println("Error: Cannot find task with ID = " + taskID);
        } catch (InvalidEntityException e) {
            System.out.println("Cannot save step.");
            System.out.println("Error: " + e.getMessage());
        }

        }


    private static void delete() {
        try {
            System.out.print("Enter Task ID to delete: ");
            int taskID = Integer.parseInt(scanner.nextLine());

            TaskService.deleteTask(taskID);

            System.out.println("Task and its steps deleted successfully.");
            System.out.println("Deleted Task ID: " + taskID);

        } catch (NumberFormatException e) {
            System.out.println("Error: Task ID must be a number!");
        } catch (EntityNotFoundException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void deleteStepsOfTask(int taskId) {
        try {
            ArrayList<Entity> allSteps = Database.getAll(2);

            boolean allDeletedSuccessfully = true;

            for (Entity entity : allSteps) {
                Step step = (Step) entity;
                if (step.getTaskRef() == taskId) {
                    try {
                        Database.delete(step.id);
                    } catch (EntityNotFoundException e) {
                        System.out.println("Warning: Step with ID " + step.id + " not found !");
                        allDeletedSuccessfully = false;
                    } catch (Exception e) {
                        System.out.println("Warning: Could not delete step with ID: " + step.id);
                        allDeletedSuccessfully = false;
                    }
                }
            }

            if (!allDeletedSuccessfully) {
                System.out.println("Warning: Some steps of task " + taskId + " could not be deleted");
            }
        } catch (Exception e) {
            System.out.println("Error: Could not retrieve steps for task " + taskId);
        }
    }

    private static void updateTask(){
        try {

            System.out.print("-ID: ");
        int taskID = Integer.parseInt(scanner.nextLine());

        System.out.print("Field : ");
        String field = scanner.nextLine().toLowerCase() ;

        System.out.print("New Value : ");
        String newValue = scanner.nextLine() ;

        Task task = (Task) Database.get(taskID);
        String oldValue = getFieldValue(task, field);

        TaskService.updateTaskField(taskID, field, newValue);

        Task updatedTask = (Task) Database.get(taskID);
        System.out.println("Successfully updated the task.");
        System.out.println("Field: " + field);
        System.out.println("Old Value: " + oldValue);
        System.out.println("New Value: " + getFieldValue(updatedTask, field));
        System.out.println("Modification Date: " + updatedTask.getLastModificationDate());

    } catch (NumberFormatException e) {
        System.out.println("Error! Task ID must be a number!");
    } catch (EntityNotFoundException e) {
        System.out.println("Error: Cannot find task with ID= " + e.getMessage());
    } catch (InvalidEntityException | IllegalArgumentException e) {
        System.out.println("Error: " + e.getMessage());
    }
    }

    private static void updateStepsStatus(int taskID , Step.Status newStatus){
        try {
            ArrayList<Entity> allSteps = Database.getAll(2);
            for (Entity entity : allSteps) {
                Step step = (Step) entity;
                if (step.getTaskRef() == taskID) {
                    step.setStatus(newStatus);
                    Database.update(step);
                }
            }
        } catch (Exception e) {
            System.out.println("Error!..... Could not update all steps of task : " + taskID);
        }
    }

    private static void updateStep() throws EntityNotFoundException, InvalidEntityException {
        System.out.print("ID: ");
        int id = Integer.parseInt(scanner.nextLine().trim());

        System.out.print("Field: ");
        String field = scanner.nextLine().trim();

        System.out.print("New Value: ");
        String newValue = scanner.nextLine().trim();

        Step step = StepService.getStep(id);
        String oldValue = getFieldValue(step, field);

        switch (field.toLowerCase()) {
            case "title":
                step.setTitle(newValue);
                break;
            case "status":
                step.setStatus(Step.Status.valueOf(newValue));
                break;
            default:
                throw new IllegalArgumentException("Invalid field: " + field);
        }

        Database.update(step);

        System.out.println("Successfully updated the step.");
        System.out.println("Field: " + field);
        System.out.println("Old Value: " + oldValue);
        System.out.println("New Value: " + getFieldValue(step, field)) ;
    }

    private static String getFieldValue(Task task, String field) {
        switch (field.toLowerCase()) {
            case "title":
                return task.getTitle();
            case "description":
                return task.getDescription();
            case "status":
                return task.getStatus().toString();
            case "duedate":
                return dateFormat.format(task.getDueDate());
            default:
                return "Unknown field";
        }
    }

    private static String getFieldValue(Step step, String field) {
        switch (field.toLowerCase()) {
            case "title": return step.getTitle();
            case "status": return step.getStatus().toString();
            default: return "Unknown field";
        }
    }

    private static void getTaskByID() {
        try {

            System.out.print("Enter Task ID: ");
            int taskID = Integer.parseInt(scanner.nextLine());

            Task task = TaskService.getTask(taskID);

            System.out.println("\nTask Details:");
            System.out.println("-----------------------------");
            System.out.println("ID: " + task.id);
            System.out.println("Title: " + task.getTitle());

            if (task.getDescription() != null && !task.getDescription().isEmpty()) {
                System.out.println("Description: " + task.getDescription());
            }

            System.out.println("Status: " + task.getStatus());
            System.out.println("Due Date: " + dateFormat.format(task.getDueDate()));
            System.out.println("Created: " + task.getCreationDate());
            System.out.println("Last Modified: " + task.getLastModificationDate());


        } catch (NumberFormatException e) {
            System.out.println("Error: Task ID must be a number!");
        } catch (EntityNotFoundException e) {
            System.out.println("Error: Task with ID " + e.getMessage() + " not found!");
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void printTaskDetails(Task task) {
        System.out.println("ID: " + task.id);
        System.out.println("Title: " + task.getTitle());
        System.out.println("Due Date: " + dateFormat.format(task.getDueDate()));
        System.out.println("Status: " + task.getStatus());

        ArrayList<Step> steps = TaskService.getStepsForTask(task.id);
        if (!steps.isEmpty()) {
            System.out.println("Steps:");
            for (Step step : steps) {
                System.out.println("    + " + step.getTitle() + ":");
                System.out.println("        ID: " + step.id);
                System.out.println("        Status: " + step.getStatus());
            }
        }
    }

    private static void getAllTasks() {
        try {
            ArrayList<Task> tasks = TaskService.getAllTasks();

            if (tasks.isEmpty()) {
                System.out.println("No tasks found.");
                return;
            }

            System.out.println("*All Tasks :");
            System.out.println("-----------------------------");

            for (Task task : tasks) {
                printTaskDetails(task);
                System.out.println();
            }

        } catch (Exception e) {
            System.out.println("Error retrieving tasks: " + e.getMessage());
        }
    }

    private static void getIncompleteTasks() {
        try {
            ArrayList<Task> incompleteTasks = TaskService.getIncompleteTasks();
             if (incompleteTasks.isEmpty()) {
                System.out.println("No incomplete tasks found.");
                return;
            }

            System.out.println("\nIncomplete Tasks:");
            System.out.println("----------------------------------------");

            for (Task task : incompleteTasks) {
                System.out.println("ID: " + task.id);
                System.out.println("Title: " + task.getTitle());
                System.out.println("Status: " + task.getStatus());
                System.out.println("Due Date: " + dateFormat.format(task.getDueDate()));

                ArrayList<Step> steps = TaskService.getStepsOfTask(task.id);
                if (!steps.isEmpty()) {
                    System.out.println("Steps:");
                    for (Step step : steps) {
                        System.out.println("  - " + step.getTitle() +
                                " (Status: " + step.getStatus() + ")");
                    }
                }
                System.out.println("----------------------------------------");
            }

        } catch (Exception e) {
            System.out.println("Error retrieving incomplete tasks: " + e.getMessage());
        }
    }

}
