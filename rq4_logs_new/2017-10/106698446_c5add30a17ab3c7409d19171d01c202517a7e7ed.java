package com.teamtreehouse.jobs;

import com.teamtreehouse.jobs.model.Job;
import com.teamtreehouse.jobs.service.JobService;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

//Pure Functions have...
//Referential Transparency: Function returns same everytime with same input.
//Memoization: Pure function outputs can be saved for each input after ran because output will never change.
//No Side Effects: Nothing outside of the function's scope gets modified even if you didn't mean to

//Lazy: Streams & functions are lazy, they wait to be called upon and only then do they do work
//State Monad: Allows a programmer to attach state information of any type to a calculation.
//Higher Order Functions: Functions that accept functions or return functions
//Function Composition: Is a function of functions.

public class App {

  public static void main(String[] args) {
    JobService service = new JobService();
    boolean shouldRefresh = false;
    try {
      if (shouldRefresh) {
        service.refresh();
      }
      List<Job> jobs = service.loadJobs();
      System.out.printf("Total jobs:  %d %n %n", jobs.size());
      explore(jobs);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void explore(List<Job> jobs) {
    Function<String, String> converter = createDateStringConverter(
            DateTimeFormatter.RFC_1123_DATE_TIME,
            DateTimeFormatter.ISO_DATE_TIME;
    );

    jobs.stream()
              .map(Job::getDateTimeString)
              .map(converter) //Final fully combined function
              .limit(5)
              .forEach(System.out::println);
  }

  public static Function<String, String> createDateStringConverter(DateTimeFormatter inFormatter, DateTimeFormatter outFormatter){
    return dateString -> LocalDateTime.parse(dateString, inFormatter)
              .format(outFormatter);
  }

  private static Function<String, String> functionalCompositionExample() {
    //Takes date string -> returns new date
    Function<String, LocalDateTime>  indeedDateConverter =
            dateString -> LocalDateTime.parse(dateString, DateTimeFormatter.RFC_1123_DATE_TIME); //Takes date string -> returns new date

    //Takes a date and returns a custom formatter string
    Function<LocalDateTime, String> siteDateStringConverter =
            date -> date.format(DateTimeFormatter.ofPattern("M / d / YY"));

    //Combines the two functions above (chaining functions)
    return indeedDateConverter.andThen(siteDateStringConverter);
  }

  private static void higherOrderFunctionExample(List<Job> jobs) {
    Job firstOne = jobs.get(0);
    System.out.println("First job: " + firstOne);
    Predicate<Job> caJobChecker = job -> job.getState().equals("CA");

    Job caJob = jobs.stream()
                    .filter(caJobChecker)
                    .findFirst()
                    .orElseThrow(NullPointerException::new);

    emailIfMatches(firstOne, caJobChecker); //method takes function as a param.
    emailIfMatches(caJob, caJobChecker.and(App::isJuniorJob));
  }

  public static void emailIfMatches(Job job, Predicate<Job> checker){
      if(checker.test(job)){ //Predicate abstract method is called test(). Runs the function on the job object
        System.out.println("I am sending and email about " + job);
      }
  }

  private static void displayCompaniesMenuUsingRange(List<String> companies) {
      IntStream.rangeClosed(1, 20)
               .mapToObj(i -> System.out.printf("%d. %s", i, companies.get(i-1)))
               .forEach(System.out::println);
  }

    private static void displayCompaniesMenuImperatively(List<String> companies) {
    for(int i = 0; i < 20; i++){
      System.out.printf("%d. %s %n", i+1, companies.get(i));
    }
  }

  private static Optional<Job> luckySearchJob(List<Job> jobs, String searchTerm) {
    return jobs.stream().filter(job -> job.getTitle().equals(searchTerm))
                                 .findFirst();
  }

  private static void longestCompanyNameStream(List<Job> jobs) {
    jobs.stream()
            .map(Job::getCompany)
//            .mapToInt(String::length) //Find the length of the company name. mapToInt gives us an IntStream now
            .max(Comparator.comparingInt(String::length)) //Finds the longest companyName then streams the string
  }

  public static Map<String, Long> getSnippetWordCountsStream(List<Job> jobs) {
      return jobs.stream()
                .map(Job::getSnippet)
                .map(snippet -> snippet.split("\\W+")) //never cross the stream, flatten it
                .flatMap(Stream::of) //Now we have a STREAM OF the WORDS only split by the map above,
                                                    //took an array of words and MADE A BRAND NEW STREAM
                .filter(word -> word.length() > 0)
                .map(String::toLowerCase)
                .collect(Collectors.groupingBy(
                      Function.identity(), //Same as saying "word -> word". CHECKS TO SEE IF THE WORD IS IN THERE
                      Collectors.counting() //INCREMENTS THE COUNT ON THE WORD COUNT KEY
                )); //Requires a function that takes an item and returns an item. Returns a map.

    //If you ever find yourself in a stream of streams then you missed a flattening step somewhere.
  }

  public static Map<String, Long> getSnippetWordCountsImperatively(List<Job> jobs) {

    Map<String, Long> wordCounts = new HashMap<>();

    for (Job job : jobs) {
      String[] words = job.getSnippet().split("\\W+");
      for (String word : words) {
        if (word.length() == 0) {
          continue;
        }
        String lWord = word.toLowerCase();
        Long count = wordCounts.get(lWord);
        if (count == null) {
          count = 0L;
        }
        wordCounts.put(lWord, ++count);
      }
    }
    return wordCounts;
  }


  private static List<Job> getThreeJuniorJobsStream(List<Job> jobs){
    return jobs.stream()
              .filter(App::isJuniorJob)
              .limit(3) //Limit is a stateful, short-circuiting, intermediate operation...it knows about the streams state
              .collect(Collectors.toList());
  }

  private static List<String> getThreeCaotionsStream(List<Job> jobs){
    return jobs.stream()
            .filter(App::isJuniorJob)
            .map(Job::getCaption) //This map method is of the function interface and MAPS/TRANSFORMS
                                  //the job object to the functions output which is a String in this case
                                  //Method Reference Inference: "Job::getCaption" expects a job object to come through stream
                                  //otherwise method must be static. Since job is expected a void method like getCaption works
                                  //since job instance has the values it needs to operate...and that's what compiler expects
            .limit(3)
            .collect(Collectors.toList());
  }

  private static boolean isJuniorJob(Job job){
    String title = job.getTitle().toLowerCase();
    return title.contains("junior") || title.contains("jr");
  }

  private static List<Job> getThreeJuniorJobsImpertively(List<Job> jobs){
    List<Job> juniorJobs = new ArrayList<>();
    for (Job job : jobs){
      if (isJuniorJob(job)){
          juniorJobs.add(job);
          if(juniorJobs.size() >= 3){
              break;
          }
      }
    }
    return juniorJobs;
  }

  private static List<String> getCaptionsImpertively(List<Job> jobs){
    List<String> captions = new ArrayList<>();
    for (Job job : jobs){
      if (isJuniorJob(job)){
        captions.add(job.getCaption());
        if(captions.size() >= 3){
          break;
        }
      }
    }
    return captions;
  }

  private static void printPortlandJobsStream(List<Job> jobs) {
    jobs.stream()
            .filter(job -> job.getState().equals("OR"))
            .filter(job -> job.getCity().equals("Portland"))
            .forEach(System.out::println);
  }

  private static void printPortlandJobsImperatively(List<Job> jobs) {
    for(Job job: jobs){
      if(job.getState().equals("OR") && job.getCity().equals("Portland")){
        System.out.println(job);
      }
    }
  }
}