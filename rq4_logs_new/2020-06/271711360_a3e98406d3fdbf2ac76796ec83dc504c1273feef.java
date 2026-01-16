package com.ravi.katas.socialnetworking;

import com.ravi.katas.socialnetworking.domain.TimelineMessage;
import com.ravi.katas.socialnetworking.service.TimeLineService;
import java.util.Optional;
import java.util.Scanner;
import java.util.Timer;
import java.util.concurrent.Delayed;

public class Driver {
 public static void main(String[] args){

   while(true) {
     Scanner input = new Scanner(System.in);
     TimeLineService timeLineService = new TimeLineService(input.nextLine());
     TimelineMessage message = new TimelineMessage(input.nextLine());
     timeLineService.publishToTimeline(message);
     System.out.println(timeLineService.viewTimeline(Optional.of(input.nextLine())));
   }
 }

}