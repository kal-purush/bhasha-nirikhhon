using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Class_Scheduler.Models;
using System.Collections.Generic;

namespace Class_Scheduler.Controllers.FileIOControllers
{
    public class FileParser 
    {
        public List<Staff> parseStaffData(List<String> fileContents)
        {
            //Create the List to represent all Staff.
            List<Staff> staffList = new List<Staff>();

            //Concat fileContents and Convert to a JSON Object using resulting String.
            JObject staffData = convertFileContents(concatFileContents(fileContents));

            //Get the Array of Staff.
            JArray staffArray = (JArray) (staffData["staff"]);

            //Iterate over the staffArray to get each staff member.
            foreach (JObject staffMember in staffArray)
            {
                //Extract all Staff Properties.

                //Extract Staff Name.
                String staffName = (String) (staffMember["name"]);

                //Extract Staff ID.
                String staffID = (String) (staffMember["id"]);

                //Extract Availability JArray.
                JArray staffAvailability = (JArray) (staffMember["availability"]);

                //Create the Dictionary to represent this staff members availability.
                //Call the parseStaffAvailability to parse staffAvailability JArray.
                Dictionary<String, List<int[]>> availability = parseStaffAvailability(staffAvailability);

                //Extract Preferences JArray.
                JArray staffPreferences = (JArray) (staffMember["preferences"]);

                //Convert preferences to a List.
                List<String> preferences = parseStaffPreferences(staffPreferences);

                //Using the Extracted Information create a Staff Object.
                Staff newStaff = new Staff(staffName, staffID, availability, preferences);

                //Add the created Staff Member to the list of Staff.
                staffList.Add(newStaff);
            }

            return staffList;  
        }

        public List<Unit> parseUnitData(List<String> fileContents)
        {
            //Create a list for all Units.
            List<Unit> unitsList = new List<Unit>();

            //Concat fileContents and convert to a JSON Object using resulting String.
            JObject unitData = convertFileContents(concatFileContents(fileContents));

            //Extract units array.
            JArray units = (JArray) (unitData["units"]);

            //Iterate over every unit.
            foreach (JObject currentUnit in units)
            {
                //Fetch the unit code.
                String unitCode = (String) (currentUnit["unitCode"]);

                //Fetch the number of tutors.
                int numTutors = (int) (currentUnit["numTutors"]);

                //Fetch the schedule array.
                JArray schedule = (JArray) (currentUnit["schedule"]);

                Dictionary<String, List<int[]>> unitSchedule = parseUnitSchedule(schedule);

                //Using the extracted information create a new unit.
                Unit newUnit = new Unit(unitCode, numTutors, unitSchedule);

                //Add the unit to the list of units.
                unitsList.Add(newUnit);
            }

            return unitsList;
        }

        public String checkDataType(List<String> fileContents)
        {
            String concattedContents = concatFileContents(fileContents);

            String dataType = "";

            if (concattedContents.Contains("staff"))
            {
                dataType = "staffData";
            }
            else if (concattedContents.Contains("units"))
            {
                dataType = "unitData";
            }
            else 
            {
                dataType = "Invalid";
            }

            return dataType;
        }

        private JObject convertFileContents(String fileContents)
        {
            JObject fileContentsAsJson = null;

            try
            {
                fileContentsAsJson = JObject.Parse(fileContents);
            }
            catch (JsonReaderException jsonReadException)
            {
                Console.WriteLine(fileContents + "\nIs  not in a valid JSON Format.");
            }

            return fileContentsAsJson;
        }

        private String concatFileContents(List<String> fileContents)
        {
            String concattedString = "";

            foreach (String temp in fileContents)
            {
                concattedString += "\n" + temp;
            }

            return concattedString;
        }

        private Dictionary<String, List<int[]>> parseStaffAvailability(JArray staffAvailability)
        {
            Dictionary<String, List<int[]>> availability = new Dictionary<String, List<int[]>>();

            //Iterate over staffAvailability to get each day.
            foreach (JObject currentDay in staffAvailability)
            {
                //Create the list to represent the array of times for this currentDay.
                List<int[]> currentDayTimes = new List<int[]>();

                //Extract the day.
                String day = (String) (currentDay["day"]);

                //Exact the JArray for times on that day.
                JArray times = (JArray) (currentDay["times"]);

                //Iterate over times to get start and finish time.
                foreach (JObject currentTime in times)
                {
                    //Extract the startTime.
                    int startTime = (int) currentTime["startTime"];

                    //Extract the endTime.
                    int endTime = (int) currentTime["endTime"];

                    //Create a new array for both times.
                    int[] newTimes = new int[2] { startTime, endTime };

                    //Store the newly created array in the times list.
                    currentDayTimes.Add(newTimes);
                }

                //Add currentDayTimes and day as a KeyValue pair to the dictionary.
                availability.Add(day, currentDayTimes);
            }

            return availability;
        }

        private List<String> parseStaffPreferences(JArray preferences)
        {
            List<String> staffPreferences = new List<String>();

            //Iterate over staffPreferences to get each unit preference.
            foreach (String currentPreference in preferences)
            {
                staffPreferences.Add(currentPreference);
            }

            return staffPreferences;
        }

        private Dictionary<String, List<int[]>> parseUnitSchedule(JArray schedule)
        {
            //Create the schedule to represent a singular unit and relevant time slots.
            Dictionary<String, List<int[]>> unitSchedule = new Dictionary<String, List<int[]>>();

            //Iterate over every day in the schedule.
            foreach (JObject currentDay in schedule)
            {
                //Used to represent the times the unit runs during a specific day.
                List<int[]> dayTimes = new List<int[]>();

                //Fetch the day name.
                String day = (String) (currentDay["day"]);

                //Fetch the times array.
                JArray times = (JArray) (currentDay["times"]);

                //Iterate over every time.
                foreach (JObject currentTime in times)
                {
                    //Fetch the start and end time.
                    int startTime = (int) (currentTime["startTime"]);
                    int endTime = (int) (currentTime["endTime"]);

                    //Store the start and end time in a new array.
                    int[] fetchedTimes = new int[2] { startTime, endTime };

                    //Add the fetchedTimes to the dayTimes list.
                    dayTimes.Add(fetchedTimes);
                }

                //Add the day and day times as KeyValue pairs in the schedule dictionary.
                unitSchedule.Add(day, dayTimes);
            }

            return unitSchedule;
        }
    }
}