package com.gslab.talent.controller;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.websocket.server.PathParam;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.gslab.talent.model.Interview;
import com.gslab.talent.repository.InterviewRepository;
import com.google.api.client.auth.oauth2.AuthorizationCodeRequestUrl;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets.Details;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.calendar.Calendar.Events;
import com.google.api.services.calendar.CalendarScopes;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.EventAttendee;
import com.google.api.services.calendar.model.EventDateTime;
import com.google.api.services.calendar.model.EventReminder;

@RestController
@CrossOrigin(allowedHeaders = "*")
public class GoogleCalController {

	private final static Log logger = LogFactory.getLog(GoogleCalController.class);
	private static final String APPLICATION_NAME = "Quickstart";
	private static HttpTransport httpTransport;
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static com.google.api.services.calendar.Calendar client;

	GoogleClientSecrets clientSecrets;
	GoogleAuthorizationCodeFlow flow;
	Credential credential;

	@Value("${google.client.client-id}")
	private String clientId;
	@Value("${google.client.client-secret}")
	private String clientSecret;
	@Value("${google.client.redirectUri}")
	private String redirectURI;

	private Set<Event> events = new HashSet<>();

	final DateTime date1 = new DateTime("2020-07-28T16:30:00.000+05:30");
	final DateTime date2 = new DateTime(new Date());

	public void setEvents(Set<Event> events) {
		this.events = events;
	}

	@RequestMapping(value = "/login/google", method = RequestMethod.GET)
	public RedirectView googleConnectionStatus(HttpServletRequest request) throws Exception {
		return new RedirectView(authorize());
	}

	@RequestMapping(value = "/", method = RequestMethod.GET, params = "code")
	public ResponseEntity<String> oauth2Callback(@RequestParam(value = "code") String code) {
		com.google.api.services.calendar.model.Events eventList;
		String message;
		System.out.println(code);
		try {
			TokenResponse response = flow.newTokenRequest(code).setRedirectUri(redirectURI).execute();
			credential = flow.createAndStoreCredential(response, "userID");
			client = new com.google.api.services.calendar.Calendar.Builder(httpTransport, JSON_FACTORY, credential)
					.setApplicationName(APPLICATION_NAME).build();
			Events events = client.events();
			eventList = events.list("primary").setTimeMin(date1).setTimeMax(date2).execute();
			message = eventList.getItems().toString();
			//System.out.println("My:" + eventList.getItems());
		} catch (Exception e) {
			logger.warn("Exception while handling OAuth2 callback (" + e.getMessage() + ")."
					+ " Redirecting to google connection status page.");
			message = "Exception while handling OAuth2 callback (" + e.getMessage() + ")."
					+ " Redirecting to google connection status page.";
		}

		//System.out.println("cal message:" + message);
		System.out.println("cal message:");
		//this.setMeeting();
		//System.out.println("after meeting set");
		return new ResponseEntity<>(message, HttpStatus.OK);
	}

	public Set<Event> getEvents() throws IOException {
		return this.events;
	}

	private String authorize() throws Exception {
		AuthorizationCodeRequestUrl authorizationUrl;
		if (flow == null) {
			Details web = new Details();
			web.setClientId(clientId);
			web.setClientSecret(clientSecret);
			clientSecrets = new GoogleClientSecrets().setWeb(web);
			httpTransport = GoogleNetHttpTransport.newTrustedTransport();
			flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets,
					Collections.singleton(CalendarScopes.CALENDAR)).build();
		}
		authorizationUrl = flow.newAuthorizationUrl().setRedirectUri(redirectURI);
		System.out.println("cal authorizationUrl->" + authorizationUrl);
		return authorizationUrl.build();
	}
	
	//newCode
	
	@Autowired
	InterviewRepository interviewRepository;
	
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public String getData()
	{
		com.google.api.services.calendar.model.Events eventList;
		Events events = client.events();
		
		 try {
			 //"2q12vhu94iroks0mjeemd3nerc@google.com"
			 System.out.println(events.get("primary", "2q12vhu94iroks0mjeemd3nerc").execute());
			 
//			 eventList = events.list("primary").setICalUID("2q12vhu94iroks0mjeemd3nerc@google.com").execute();			 
//			 eventList.getItems().forEach(e -> {
//				 System.out.println(e.getICalUID());
//				 System.out.println(e.getId());
//				 System.out.println(e.getStatus());
//				 System.out.println(e.getSummary());
//				 System.out.println(e.getAttendees());
//				 });
		} catch (IOException e) {
			System.out.println("--------------------");
			e.printStackTrace();
		}
		 return "abc";
	}
	
	private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
	
	//@Scheduled(cron = "0 */5 * * * *")
	@RequestMapping(value = "/setresponse", method = RequestMethod.GET)
	public void setResponse()
	{
		
		Events events = client.events();
		
		interviewRepository.findAll().forEach(i->{
			if(i.getInterviewStatus().equals("waiting"))
			{
			Event e=null;	
			try {
				
				System.out.println(i.getCalEventId());
			 e = events.get("primary",i.getCalEventId()).execute();
			} catch (IOException exp) {	
				exp.printStackTrace();
			}
			System.out.println(e);
			System.out.println(i.getPanelEmail());
			e.getAttendees().forEach(atd->{
				
				
				if(atd.getEmail().equals(i.getPanelEmail()))
				{
	
					i.setPanelResponseStatus(atd.getResponseStatus());
				}else if(atd.getEmail().equals(i.getCandidateEmail()))
				{
					i.setCandidateResponseStatus(atd.getResponseStatus());
				}
			});
			if(i.getPanelResponseStatus().equals("accepted") && i.getCandidateResponseStatus().equals("accepted"))
			{
				e.setSummary("interview is conformd");
				try {
					events.update("primary", e.getId(), e).setSendNotifications(true).execute();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				i.setInterviewStatus("scheduled");
			}
			interviewRepository.save(i);
			}			
		});
		System.out.println(dateFormat.format(new Date()));
	}

	@RequestMapping(value = "/setMeeting", method = RequestMethod.POST)
	public String setMeeting(@RequestBody Interview it)
	{
		it.setPanelEmail("dakhilesh95@gmail.com");
		it.setCandidateEmail("deshmukha816@gmail.com");
		System.out.println(it);
		Event event = new Event();
	    event.setSummary("Interview Call From GSlab, give response in the form of yes or no")
	    .setLocation("Amar Arma Genesis, Baner Rd, Baner, Pune, Maharashtra 411045")
	    .setDescription("Interview for software Engg");

	DateTime startDateTime = new DateTime(it.getScheduledOn());
	EventDateTime start = new EventDateTime()
	    .setDateTime(startDateTime)
	    .setTimeZone("Asia/Kolkata");
	
	event.setStart(start);
	
	DateTime endDateTime = new DateTime(it.getScheduledEndTime());
	EventDateTime end = new EventDateTime()
	    .setDateTime(endDateTime)
	    .setTimeZone("Asia/Kolkata");
	 event.setEnd(end);

	String[] recurrence = new String[] {"RRULE:FREQ=DAILY;COUNT=1"};
	event.setRecurrence(Arrays.asList(recurrence));

	EventAttendee[] attendees = new EventAttendee[] {
		new EventAttendee().setOrganizer(true).setEmail("akhilesh.deshmukh@gslab.com"),
	    new EventAttendee().setEmail("dakhilesh95@gmail.com"),
	    new EventAttendee().setEmail("deshmukha816@gmail.com"),
	};
	
	event.setAttendees(Arrays.asList(attendees));
	
	event.setOriginalStartTime(start);
	event.setVisibility("private");
	event.setGuestsCanModify(false);
	event.setGuestsCanInviteOthers(false);
	
	EventReminder[] reminderOverrides = new EventReminder[] {
	    new EventReminder().setMethod("email").setMinutes(24 * 60),
	    new EventReminder().setMethod("popup").setMinutes(10),
	};
	
	Event.Reminders reminders = new Event.Reminders()
 	     .setUseDefault(false)
	    .setOverrides(Arrays.asList(reminderOverrides));
	 event.setReminders(reminders);

	 //event.setStatus("confirmed");
	String calendarId = "primary";
	System.out.println(event.getId());
	Event eventOutput=null; 
	 try {
	    eventOutput=client.events().insert(calendarId, event).setSendNotifications(true).execute();
	} catch (IOException e) {
		e.printStackTrace();
	}
	 
	System.out.printf("Event created: %s\n", eventOutput.getId());	
	it.setCalEventId(eventOutput.getId());
	it.setCandidateResponseStatus("needsAction");
	it.setPanelResponseStatus("needsAction");
	it.setInterviewStatus("waiting");
	interviewRepository.save(it);
	System.out.printf("Event created: %s\n", eventOutput.getStart());
	
	return "event created";
	}
	
	@RequestMapping(value = "/UpdateMeeting", method = RequestMethod.POST)
	public String rescheduleMeeting(@RequestBody Interview it)
	{
		Events events = client.events();
		Interview i=interviewRepository.findById(it.getInterviewId()).get();
	String eventId=i.getCalEventId();
	Event event=null;	
	try {
	 event = events.get("primary",eventId).execute();
	} catch (IOException exp) {	
		exp.printStackTrace();
	}
	System.out.println(event);
	
	event.setSummary("Interview Call From GSlab, give response in the form of yes or no")
    .setLocation("Amar Arma Genesis, Baner Rd, Baner, Pune, Maharashtra 411045")
    .setDescription("Interview for software Engg");

		DateTime startDateTime = new DateTime(it.getScheduledOn());
		EventDateTime start = new EventDateTime()
		    .setDateTime(startDateTime)
		    .setTimeZone("Asia/Kolkata");
		
		event.setStart(start);
		
		DateTime endDateTime = new DateTime(it.getScheduledEndTime());
		EventDateTime end = new EventDateTime()
		    .setDateTime(endDateTime)
		    .setTimeZone("Asia/Kolkata");
		 event.setEnd(end);
		
		String[] recurrence = new String[] {"RRULE:FREQ=DAILY;COUNT=1"};
		event.setRecurrence(Arrays.asList(recurrence));
		
		EventAttendee[] attendees = new EventAttendee[] {
			new EventAttendee().setOrganizer(true).setEmail("akhilesh.deshmukh@gslab.com"),
		    new EventAttendee().setEmail("dakhilesh95@gmail.com"),
		    new EventAttendee().setEmail("deshmukha816@gmail.com"),
		};
		
		event.setAttendees(Arrays.asList(attendees));
		
		event.setOriginalStartTime(start);
		event.setVisibility("private");
		event.setGuestsCanModify(false);
		event.setGuestsCanInviteOthers(false);
		
//		EventReminder[] reminderOverrides = new EventReminder[] {
//		    new EventReminder().setMethod("email").setMinutes(24 * 60),
//		    new EventReminder().setMethod("popup").setMinutes(10),
//		};
//		
//		Event.Reminders reminders = new Event.Reminders()
//			     .setUseDefault(false)
//		    .setOverrides(Arrays.asList(reminderOverrides));
//		 event.setReminders(reminders);
		
		 //event.setStatus("confirmed");
		String calendarId = "primary";
		System.out.println(event.getId());
		Event eventOutput=null; 
		 try {
		    eventOutput=client.events().update(calendarId,eventId, event).setSendNotifications(true).execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
		System.out.printf("Event created: %s\n", eventOutput.getId());	
		i.setPanelEmail("dakhiles95@gmail.com");
		i.setCandidateEmail("deshmukha816@gmail.com");
		i.setScheduledOn(it.getScheduledOn());
		i.setScheduledEndTime(it.getScheduledEndTime());
		i.setCalEventId(eventOutput.getId());
		i.setCandidateResponseStatus("needsAction");
		i.setPanelResponseStatus("needsAction");
		i.setInterviewStatus("waiting");
		interviewRepository.save(i);
		System.out.printf("Event created: %s\n", eventOutput.getStart());
		
		return "event updated";
	}
	
	@DeleteMapping("/deleteMeeting/{interviewId}")
	public void deleteEvent(@PathVariable(value = "interviewId") Long interviewId)
	{
		String eventId= interviewRepository.findById(interviewId).get().getCalEventId();
		Event event=null;	
		try {
		 client.events().delete("primary",eventId).setSendNotifications(true).execute();
		} catch (IOException exp) {	
			exp.printStackTrace();
		}	
	interviewRepository.deleteById(interviewId);
	}

	@GetMapping("/interview")
	public List<?> getAllEvent()
	{
		List<Interview> list=new ArrayList<Interview>();
	interviewRepository.findAll().forEach(i->{
		if(i.getInterviewStatus().equals("waiting"))
		{
			list.add(i);
		}
	});
	return list;
	}
	
}