using System;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.Resource.Fluent.Authentication;
using Microsoft.Azure.Management.Resource.Fluent.Core;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using System.Diagnostics;
using System.Linq;

namespace ConsoleApplication
{
    public class Program
    {
        public static async void AssertVms(string subscriptionId,bool simulate) {
            
            AzureCredentials credentials = AzureCredentials.FromFile("./cred.azure");
            
            var azure = Azure
                    .Configure()
                    .WithLogLevel(HttpLoggingDelegatingHandler.Level.NONE)
                    .Authenticate(credentials)
                    .WithSubscription(subscriptionId);

            var azureVms = azure.VirtualMachines.List();
            azureVms.LoadAll();

            var parallelOptions = new ParallelOptions();
            parallelOptions.MaxDegreeOfParallelism = 50;

            Parallel.ForEach(azureVms, parallelOptions, (vm) =>{
                if(vm.Tags.ContainsKey("AutoShutdownSchedule"))
                {
                    Debug.WriteLine($"[Trace] Auto-Shutdown found on vm {vm.Name}");
                    var powerState = vm.PowerState.ToString().Replace("PowerState/","");
                    var schedules = vm.Tags["AutoShutdownSchedule"].Split(',');
                    var shouldBeOff = schedules.Any(x => CheckScheduleEntry(x));
                    if(shouldBeOff && powerState.Contains("running"))
                    {
                        Debug.WriteLine($"[Trace - {vm.Name}] Shutting down VM");
                        if (!simulate) {
                            Task.Run(() => {azure.VirtualMachines.Deallocate(vm.ResourceGroupName,vm.Name);});
                        }
                    } else if(!shouldBeOff && powerState.Contains("deallocated")) {
                        Debug.WriteLine($"[Trace - {vm.Name}] Starting VM");
                        if(!simulate)  {
                            Task.Run(() => {azure.VirtualMachines.Start(vm.ResourceGroupName,vm.Name);});
                        }
                    }
                    
                } else {
                    Debug.WriteLine($"[Trace] Auto-Shutdown not found on vm {vm.Name}");
                }                
            });
        }
        public static void Main(string[] args)
        {
            Console.WriteLine("Starting to verify machine state");
            var subscriptions = new []{
                "904ad912-63ba-41fd-bb70-c7d23b5c8674",
                "af515c4f-82cd-4dc7-a360-8c855842ff60",
                "4b26ca69-1cf0-44c9-91cc-1ba482f8b06e",
                "60881ba6-11ee-4eeb-a90c-6e6e218ba086",
                "97d3edd9-93d4-4b12-bbe3-6c41922a9b8c",
                "b1921b69-4667-4cc6-a9e7-5518f4a80ca1",
                "c6b7d7fc-ddbd-4619-bbd5-75122dc8634e",
                "d3579878-2c08-419b-bf49-42251129a424",
                "0c62076e-ddf0-4dbe-8470-bc40784f0fea",
                "206878b8-5f28-476e-acc8-1cff1b03efe3",
                "22f1bdf4-3e6d-4fa4-8c0a-fa5940198e33",
                "5158e0fd-394a-4434-9908-8bf7ecad69ea",
                "999bf067-9b94-48e9-be87-507a01a152ab",
                "c44f9eb0-3180-4942-8315-612831fd7d3f",
                "f4d34235-c902-4739-b830-84a575ff6e50",
                "f5a9b9dd-88a8-4e1d-87ab-2ca3a19a08a0"
            };
            foreach(var subscription in subscriptions){
                AssertVms(subscription,true);
            }
        }
        static bool CheckScheduleEntry (string timeRange)
		{	
			var currentTime = DateTime.UtcNow;
    		var midnight = DateTime.UtcNow.AddDays(1).Date;	 
            DateTime? rangeStart = null,rangeEnd = null,parsedDay = null;
            if( string.IsNullOrWhiteSpace(timeRange) || 
                timeRange.Equals("DoNotShutDown",StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
			try
			{
	    		// Parse as range if contains '->'
                if(timeRange.Contains("->"))
	    		{
                    var timeRangeComponents = Regex.Split(timeRange,"->");
	        		if(timeRangeComponents.Length == 2)
	        		{
                        rangeStart = DateTime.Parse(timeRangeComponents[0]);
                        rangeEnd = DateTime.Parse(timeRangeComponents[1]);
			
	            		// Check for crossing midnight
	            		if(rangeStart > rangeEnd)
	            		{
                    		// If current time is between the start of range and midnight tonight, interpret start time as earlier today and end time as tomorrow
                    		if(currentTime > rangeStart && currentTime < midnight)
                    		{
                        		rangeEnd = rangeEnd.Value.AddDays(1);
                    		}
                    		// Otherwise interpret start time as yesterday and end time as today   
                    		else
                    		{
                        		rangeStart = rangeStart.Value.AddDays(-1);
                    		}
	            		}
	        		}
	        		else
	        		{
                        Console.WriteLine("WARNING: Invalid time range format. Expects valid .Net DateTime-formatted start time and end time separated by '->'"); 
	        		}
	    		}
	    		// Otherwise attempt to parse as a full day entry, e.g. 'Monday' or 'December 25' 
	    		else
	    		{
                    DayOfWeek dayOfWeek;
                    var isDay = Enum.TryParse(timeRange,out dayOfWeek);
                    if(isDay)
                    {
                        // If specified as day of week, check if today
                        if(dayOfWeek == System.DateTime.UtcNow.DayOfWeek)
                        {
                            parsedDay = DateTime.UtcNow.Date;
                        }
                    }
	        		// Otherwise attempt to parse as a date, e.g. 'December 25'
	        		else {
                        parsedDay = DateTime.Parse(timeRange);
	        		}	    		
	        		if(parsedDay != null)
	        		{
	            		rangeStart = parsedDay; //# Defaults to midnight
	            		rangeEnd = parsedDay.Value.AddHours(23).AddMinutes(59).AddSeconds(59); //# End of the same day
	        		}
	    		}
                if(rangeStart.HasValue && rangeEnd.HasValue){
                    // Check if current time falls within range
                    return currentTime > rangeStart.Value && currentTime < rangeEnd.Value;
                } else {
                    return false;
                }
			}
			catch (Exception ex)
			{
	    		// Record any errors and return false by default
	    		Console.WriteLine($"WARNING: Exception encountered while parsing time range. Details: {ex.Message}. Check the syntax of entry, e.g. '<StartTime> -> <EndTime>', or days/dates like 'Sunday' and 'December 25'");
	    		return false;
			}
			
		}
    }
}