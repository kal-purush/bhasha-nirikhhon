/*
 * This file is part of the Buildings and Habitats object Model (BHoM)
 * Copyright (c) 2015 - 2022, the respective contributors. All rights reserved.
 *
 * Each contributor holds copyright over their respective contributions.
 * The project versioning (Git) records all such contribution source information.
 *                                           
 *                                                                              
 * The BHoM is free software: you can redistribute it and/or modify         
 * it under the terms of the GNU Lesser General Public License as published by  
 * the Free Software Foundation, either version 3.0 of the License, or          
 * (at your option) any later version.                                          
 *                                                                              
 * The BHoM is distributed in the hope that it will be useful,              
 * but WITHOUT ANY WARRANTY; without even the implied warranty of               
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the                 
 * GNU Lesser General Public License for more details.                          
 *                                                                            
 * You should have received a copy of the GNU Lesser General Public License     
 * along with this code. If not, see <https://www.gnu.org/licenses/lgpl-3.0.html>.      
 */

using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Collections;
using System.ComponentModel;
using BH.oM.Base.Attributes;
using BH.Engine.Serialiser;
using BH.oM.Base;
using UT = BH.oM.Test.UnitTests;
using BH.oM.Test.Results;
using BH.Engine.Base;
using BH.oM.UnitTest.Results;
using BH.Engine.Test;
using BH.oM.Data.Library;
using System.IO;

namespace BH.Engine.UnitTest
{
    public static partial class Query
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("Runs and checks all provided UnitTests and provides the ones that has a the provided status.")]
        [Input("test", "The tests to filter.")]
        [Input("status", "Status looked for.")]
        [Input("filterData", "If true, returns new UnitTests objects where the data has been filtered to only contain TestData which returns the provided status. If false, returns full provided UnitTest if the overall status is matching the provided status.")]
        [MultiOutput(0, "tests", "The filtered tests.")]
        [MultiOutput(1, "results", "Results matching the filtered tests.")]
        public static Output<List<UT.UnitTest>, List<TestResult>> FilterByCheckStatus(this List<UT.UnitTest> tests, BH.oM.Test.TestStatus status = oM.Test.TestStatus.Error, bool filterData = false)
        {
            tests = tests.Where(x => x != null).ToList();   //Filter out nulls

            //Return collections
            List<UT.UnitTest> filteredTests = new List<UT.UnitTest>();
            List<TestResult> filteredResults = new List<TestResult>();

            if (filterData) //If filterData is true, some work is required
            {
                foreach (UT.UnitTest test in tests) //Loop through all tests
                {
                    TestResult result = test.CheckTest();   //Get result from check test

                    if (result.Information.All(x => x.Status == status))    //If all inner reuslts match the status, the full test and result can be outputed
                    {
                        filteredTests.Add(test);
                        filteredResults.Add(result);
                    }
                    else    //If not, need to investigate inner
                    {
                        UT.UnitTest filterTest = new UT.UnitTest { Method = test.Method };  //Halfclone of the UT, keeping only the method
                        TestResult filterResult = result.ShallowClone();    //Clone the result and wipe inner TestInformation
                        filterResult.Information = new List<oM.Test.ITestInformation>();
                        for (int i = 0; i < test.Data.Count; i++)   //For each innner TestData point
                        {
                            if (result.Information[i].Status == status) //Status matching -> add Data to UT and test information to result
                            {
                                filterTest.Data.Add(test.Data[i]);
                                filterResult.Information.Add(result.Information[i]);
                            }
                        }
                        if (filterTest.Data.Count != 0) //At least one inner match found
                        {
                            filteredTests.Add(filterTest);
                            filteredResults.Add(filterResult);
                        }
                    }
                }
            }
            else    //If not filtering by data, we are only interesed in the outer test result status
            {
                for (int i = 0; i < tests.Count; i++)   //Simly loop through the tests
                {
                    TestResult result = tests[i].CheckTest();   
                    if (result.Status == status)    //And add if matching outer status
                    {
                        filteredTests.Add(tests[i]);
                        filteredResults.Add(result);
                    }
                }
            }

            return new Output<List<UT.UnitTest>, List<TestResult>> { Item1 = filteredTests, Item2 = filteredResults };
        }

        /***************************************************/

        [Description("Runs and checks all provided UnitTests in the provided Dataset and provides the ones that has a the provided status.")]
        [Input("testDataSet", "The test dataset to be evaluated.")]
        [Input("status", "Status looked for.")]
        [Input("filterData", "If true, returns new UnitTests objects where the data has been filtered to only contain TestData which returns the provided status. If false, returns full provided UnitTest if the overall status is matching the provided status.")]
        [MultiOutput(0, "tests", "The filtered tests.")]
        [MultiOutput(1, "results", "Results matching the filtered tests.")]
        public static Output<List<UT.UnitTest>, List<TestResult>> FilterByCheckStatus(this Dataset testDataSet, BH.oM.Test.TestStatus status = oM.Test.TestStatus.Error, bool filterData = false)
        {
            if (testDataSet == null)
                return new Output<List<UT.UnitTest>, List<TestResult>> { Item1 = new List<UT.UnitTest>(), Item2 = new List<TestResult>() };
                
            List<UT.UnitTest> unitTests = testDataSet.Data.OfType<UT.UnitTest>().ToList();
            return FilterByCheckStatus(unitTests, status, filterData);
        }

        /***************************************************/

        [Description("Runs and checks all provided UnitTests and provides the ones that has a the provided status.")]
        [Input("fileName", "The full file path to the file containing the serialised test datasets.")]
        [Input("status", "Status looked for.")]
        [Input("filterData", "If true, returns new UnitTests objects where the data has been filtered to only contain TestData which returns the provided status. If false, returns full provided UnitTest if the overall status is matching the provided status.")]
        [MultiOutput(0, "tests", "The filtered tests.")]
        [MultiOutput(1, "results", "Results matching the filtered tests.")]
        public static Output<List<UT.UnitTest>, List<TestResult>> FilterByCheckStatus(this string fileName, BH.oM.Test.TestStatus status = oM.Test.TestStatus.Error, bool filterData = false)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                Base.Compute.RecordError("No filename provided.");
                return new Output<List<UT.UnitTest>, List<TestResult>> { Item1 = new List<UT.UnitTest>(), Item2 = new List<TestResult>() };
            }
            StreamReader sr = new StreamReader(fileName);
            string line = sr.ReadToEnd();
            sr.Close();

            object ds = Serialiser.Convert.FromJson(line);
            if (ds == null)
            {
                Base.Compute.RecordError("Dataset did not deserialise correctly.");
                return new Output<List<UT.UnitTest>, List<TestResult>> { Item1 = new List<UT.UnitTest>(), Item2 = new List<TestResult>() };
            }

            Dataset testSet = ds as Dataset;
            if (testSet == null)

            {
                Base.Compute.RecordError("Dataset did not deserialise correctly as a BHoM Dataset.");
                return new Output<List<UT.UnitTest>, List<TestResult>> { Item1 = new List<UT.UnitTest>(), Item2 = new List<TestResult>() };
            }

            return FilterByCheckStatus(testSet, status, filterData);
        }

        /***************************************************/
    }
}