using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Macquarie.Handbook;
using Macquarie.Handbook.Data;
using Macquarie.Handbook.Data.Shared;
using Macquarie.Handbook.WebApi;
using Unit_Info.Helpers;
using static Macquarie.JSON.JsonSerialisationHelper;
using static Unit_Info.Helpers.LocalDataDirectoryHelper;
using static Unit_Info.Helpers.LocalDirectories;

namespace Unit_Info.Demonstrations
{
    public class Demo
    {
        static readonly IMacquarieHandbook _handbook = new MacquarieHandbook(default);

        /// <summary>
        /// Demonstrates Unit request API creation, data collection and access.
        /// </summary>
        public static async Task GetUnit(String unitCode, bool writeToFile = true) {
            var unit = await _handbook.GetUnit(unitCode, DateTime.Now.Year);

            if (unit is null) {
                Console.WriteLine("Unit with code '{0}' was not found.", unitCode);
                return;
            }

            Console.WriteLine(unit.UnitData.Title);

            if (writeToFile)
                await WriteObjectToFile(unit, LocalDirectories.Unit_Individual);

        }

        /// <summary>
        /// Demonstrates Unit request API creation, data collection and access.
        /// Requests all 2021 Units, with a limit of 3000 results.
        /// </summary>
        public static async Task GetAllUnitsAndWriteToFile() {
            var unitCollection = await _handbook.GetAllUnits(2021, 10);

            await WriteCollectionToIndividualFiles(unitCollection);

            Console.WriteLine($"{unitCollection.Count} unit{(unitCollection.Count > 0 ? "s" : "")} retrieved and written to disk.");
        }

        private static async Task WriteObjectToFile(MacquarieMetadata data, LocalDirectories directoryToSaveTo, bool saveWithTimeStamp = false, Newtonsoft.Json.Formatting formatting = Newtonsoft.Json.Formatting.Indented) {
            if (data is not null) {
                await SerialiseObjectToJsonFile(data, CreateFilePath(directoryToSaveTo, data.Code), saveWithTimeStamp, formatting);
            }
        }

        private static async Task WriteCollectionToIndividualFiles<T>(MacquarieDataCollection<T> collection) where T : MacquarieMetadata {
            foreach (var unit in collection) {
                await WriteObjectToFile(unit, LocalDirectories.Unit_Individual, false, Newtonsoft.Json.Formatting.None);
            }
        }


        /// <summary>
        /// Demonstrates Course  data collection and access.
        /// </summary>
        /// <param name="courseCode">
        /// The course code to attempt to retreive.
        /// </param> 
        public static async Task<MacquarieCourse> GetCourse(String courseCode) {
            var course = await _handbook.GetCourse(courseCode, 2021);

            if (course is null) {
                Console.WriteLine($"No course with code '{courseCode}' was found.");
                return null;
            }

            Console.WriteLine(course.CourseData.CourseSearchTitle);
            await SerialiseObjectToJsonFile(course, CreateFilePath(Course_Individual, courseCode));
            return course;
        }

        public static async Task<IEnumerable<MacquarieCourse>> GetAllCourses() {
            var courses = await _handbook.GetAllCourses(2021);
            return courses.AsEnumerable();
        }

        public static async Task GetAllCoursesAndSaveGroupedBySchool() {
            var courseCollection = await GetAllCourses();

            if (courseCollection.Any()) {
                await WriteCoursesGroupedBySchool(courseCollection);
            }
        }


        /// <summary>
        /// Demonstrates Course request API creation, data collection and access.
        /// Requests all 2021 courses..
        /// </summary>
        public static async Task GetAllCoursesAndWriteToFile() {
            var courseCollection = await GetAllCourses();

            if (!courseCollection.Any()) {
                Console.WriteLine($"No courses were found.");
                return;
            }

            await WriteCoursesToIndividualFiles(courseCollection);
        }

        public static async Task WriteUnitsToIndividualFilesFiltered(IEnumerable<MacquarieUnit> units) {
            var schoolGroups = units.AsEnumerable().GroupBy(u => u.UnitData.School.Value);

            foreach (var group in schoolGroups) {
                var depts = group.GroupBy(u => u.UnitData.AcademicOrganisation.Value);

                foreach (var department in depts) {
                    var levels = department.GroupBy(u => u.Level);

                    foreach (var level in levels) {
                        foreach (var unit in level) {
                            var parentDir = CreateFilePath(Unit_Filtered_BySchool, $"{group.Key}/{department.Key.TrimEnd()}/");
                            var childpath = $"{level.Key}/{unit.Code}";
                            string filePath = parentDir + childpath;

                            LocalDataMap.Register(unit.Code, parentDir);
                            await SerialiseObjectToJsonFile(unit, filePath);
                        }
                    }
                }
            }
        }

        public static async Task WriteCoursesGroupedBySchool(IEnumerable<MacquarieCourse> courses) {
            if (courses is not null && courses.Any()) {
                var grouped = courses.AsEnumerable().OrderBy(crs => crs.Code).GroupBy(crs => crs.CourseData.School.Value);

                foreach (var g in grouped) {
                    await SerialiseObjectToJsonFile(g, CreateFilePath(Course_Filtered_BySchool, g.Key));
                }
            }
        }

        public static async Task GetAllUnitNames() {
            var unitCollection = await _handbook.GetAllUnits(2021);
            List<String> unitNames = new(unitCollection.Count);
            foreach (var unit in unitCollection) {
                unitNames.Add(unit.Code);
            }

            await SerialiseObjectToJsonFile(unitNames, CreateFilePath(Unit_Filtered, "UnitNames"));
        }

        //Downloads all units and saves a copy of only the prerequisite enrolment rules.
        public static async Task GetAllUnitPrerequsiteForDevelopment() {
            Stopwatch sw = new();

            sw.Restart();
            var unitCollection = await _handbook.GetAllUnits(2021);

            if (unitCollection.Count > 0) {
                var ruleAndCode = new List<Tuple<string, string>>();
                foreach (var i in unitCollection) {
                    foreach (var j in i.UnitData.EnrolmentRules) {
                        if (j.Type.Value == "prerequisite") {
                            ruleAndCode.Add(new Tuple<string, string>(i.Code, j.Description));
                        }
                    }
                }

                var orderedList = ruleAndCode.OrderBy(i => i.Item2.Length);

                await SerialiseObjectToJsonFile(orderedList, CreateFilePath(Unit_PreRequisite_Unparsed, "Macquarie_EnrolmentRules_ASC_LENGTH"));

                string[] prereqStrings = (from item in orderedList
                                          select item.Item2).ToArray();

                HashSet<String> hashset = new();

                foreach (var (item1, item2) in orderedList) {
                    hashset.Add(item2);
                }

                await SerialiseObjectToJsonFile(hashset, CreateFilePath(Unit_PreRequisite_Unparsed, "Unique_MQEnrolmentRules"));

                int hashsetSize = hashset.Count;
                List<String> tenPercentSelection = new(hashsetSize / 10);
                for (int i = 0; i < hashsetSize; i += 10) {
                    if (i < hashsetSize)
                        tenPercentSelection.Add(hashset.ElementAt(i));
                }

                await SerialiseObjectToJsonFile(tenPercentSelection, CreateFilePath(Unit_PreRequisite_Unparsed, "Unique_MQEnrolmentRules_Selection"));
            }

            sw.Stop();

            Console.WriteLine("{0} milliseconds for {1} unit query & deserialisation.", sw.ElapsedMilliseconds, unitCollection.Count);
        }

        public static async Task ProcessPrereqs() {
            // string filePathUniqueEnrolmentRules = Environment.OSVersion.Platform switch
            // {
            //     PlatformID.Unix => $"/home/harley/Documents/Repos/MQUni/Unit-Tools/Projects/Unit Info/data/units/prerequisites/unparsed/Unique_MQEnrolmentRules.json",
            //     PlatformID.Win32NT => "Projects\\Unit Info\\data\\units\\prerequisites\\unparsed\\Unique_MQEnrolmentRules.json",
            //     _ => throw new InvalidProgramException("Only Linux and Windows are supported by function.")
            // };

            string dataPath = $"{Environment.CurrentDirectory}\\{CreateFilePath(Unit_PreRequisite_Unparsed, "Unique_MQEnrolmentRules.json")}";

            string[] prereqs = DeserialiseJsonObject<string[]>(await File.ReadAllTextAsync(dataPath));


            // Unique requirements containing 'admission'
            var admissionVarientFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "MQEnrolmentRules_Admission_Varients_Selection");
            if (!File.Exists(admissionVarientFilePath)) {
                IEnumerable<String> filtered = from str in prereqs
                                               where str.Contains("dmission")
                                               select str;

                List<String> selection = new();
                for (int i = 0; i < filtered.Count(); i += 10) {
                    if (i < filtered.Count())
                        selection.Add(filtered.ElementAt(i));
                }

                await SerialiseObjectToJsonFile(selection, admissionVarientFilePath);

                // Words following 'admission'
                var admissionFollowingWordsFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_Admission_FollowingWord");
                if (!File.Exists(admissionFollowingWordsFilePath)) {
                    HashSet<String> wordsFollowingAdmission = new();

                    foreach (var str in filtered) {
                        int idx = str.IndexOf("dmission");
                        if (idx >= 0) {
                            var subStr = str[(idx + 8)..];//does substring. Range operator.
                            var split = subStr.Trim().Split(' ', 2);
                            if (split.Length > 0) {
                                var combined = "Admission " + split[0];
                                wordsFollowingAdmission.Add(combined);
                            }
                        }
                    }

                    await SerialiseObjectToJsonFile(wordsFollowingAdmission, admissionFollowingWordsFilePath);
                }
            }

            // Words following 'and'
            var andFollowingWordFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_And_FollowingWord");

            if (!File.Exists(andFollowingWordFilePath)) {
                HashSet<String> wordsFollowingAnd = new();

                foreach (var str in prereqs) {
                    var split = str.Split(" and ");
                    for (int i = 1; i < split.Length; i += 2) {
                        wordsFollowingAnd.Add("and " + split[i].Trim().Split()?[0]);
                    }
                }

                await SerialiseObjectToJsonFile(wordsFollowingAnd, andFollowingWordFilePath);
            }

            // Words following 'or'
            var orFollowingWordFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_Or_FollowingWord");
            if (!File.Exists(orFollowingWordFilePath)) {
                HashSet<String> wordsFollowingOr = new();

                foreach (var str in prereqs) {
                    var split = str.Split(" or ");
                    for (int i = 1; i < split.Length; i += 2) {
                        wordsFollowingOr.Add("or " + split[i].Trim().Split()?[0]);
                    }
                }

                await SerialiseObjectToJsonFile(wordsFollowingOr, orFollowingWordFilePath);
            }

            // Words following 'including'
            var includingFollowingWordFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_Including_FollowingWord");
            if (!File.Exists(includingFollowingWordFilePath)) {
                HashSet<String> wordsFollowingIncluding = new();
                foreach (var str in prereqs) {
                    var split = str.Split("including");
                    for (int i = 1; i < split.Length; i += 2) {
                        wordsFollowingIncluding.Add("including " + split[i].Trim().Split()?[0]);
                    }
                }

                await SerialiseObjectToJsonFile(wordsFollowingIncluding, includingFollowingWordFilePath);
            }

             // Requirements containing 'corequisite'
            var includingCorequisiteFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_Corequisite_Unique");
            if (!File.Exists(includingCorequisiteFilePath)) {
                HashSet<String> includingCorequisite  = new();
                foreach (var str in prereqs) {
                    if (str.Contains("corequisite")) {
                        includingCorequisite.Add(str);
                    }
                }

                await SerialiseObjectToJsonFile(includingCorequisite, includingCorequisiteFilePath);
            }

             // Requirements containing 'completion'
            var includingCompletionFilePath = CreateFilePath(Unit_PreRequisite_Unparsed, "Keyword_Completion_Unique");
            if (!File.Exists(includingCompletionFilePath)) {
                HashSet<String> includingCompletion  = new();
                foreach (var str in prereqs) {
                    if (str.Contains("completion", StringComparison.OrdinalIgnoreCase)) {
                        includingCompletion.Add(str);
                    }
                }

                await SerialiseObjectToJsonFile(includingCompletion, includingCompletionFilePath);
            }
        }

        public static async Task SaveListOfUnitCodesAndTitles() {
            var unitCodes = await GetListOfUnitCodes();

            await SerialiseObjectToJsonFile(unitCodes, CreateFilePath(Unit_Filtered, "Macquarie_Unit_Codes"));
        }

        public static async Task<IEnumerable<IGrouping<string, MacquarieBasicItemInfo>>> GetListOfUnitCodes() {
            var unitCollection = await _handbook.GetAllUnits();

            var enumerable = from unit in unitCollection.AsEnumerable()
                             select new { unit.Code, unit.Title, unit.UnitData.School.Value };
            var query = enumerable.Select(item => new MacquarieBasicItemInfo(item.Code, item.Title, item.Value)).OrderBy(item => item.Code).GroupBy(item => item.Department);

            return query;
        }

        public static async Task SaveListOfCourseCodesAndTitles() {
            var courseCodes = await GetListOfCourseCodes();

            await SerialiseObjectToJsonFile(courseCodes, CreateFilePath(Course_Filtered, "Macquarie_Course_Codes"));
        }

        public static async Task<IEnumerable<IGrouping<string, MacquarieBasicItemInfo>>> GetListOfCourseCodes() {
            var courseCollection = await _handbook.GetAllCourses();

            var enumerable = (from course in courseCollection.AsEnumerable()
                              select new { course.Code, course.Title, course.CourseData.School.Value });
            var query = enumerable.Select(item => new MacquarieBasicItemInfo(item.Code, item.Title, item.Value)).OrderBy(item => item.Code).GroupBy(item => item.Department);

            return query;
        }

        public static async Task WriteCoursesToIndividualFiles(IEnumerable<MacquarieCourse> courses) {
            foreach (var course in courses) {
                await SerialiseObjectToJsonFile(course, CreateFilePath(Course_Individual, course.Code));
            }
        }

        public static async Task WriteUnitsToIndividualFiles(IEnumerable<MacquarieUnit> units) {
            foreach (var unit in units) {
                await SerialiseObjectToJsonFile(unit, CreateFilePath(Unit_Individual, unit.Code));
            }
        }
    }

    public struct MacquarieBasicItemInfo
    {
        public string Code { get; set; }
        public string Title { get; set; }
        public string Department { get; set; }

        public MacquarieBasicItemInfo(string pCode, string pTitle, string pDepartment) {
            this.Code = pCode;
            this.Title = pTitle;
            this.Department = pDepartment;
        }
    }
}