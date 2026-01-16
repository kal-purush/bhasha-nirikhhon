/*
 * This file is part of the Buildings and Habitats object Model (BHoM)
 * Copyright (c) 2015 - 2020, the respective contributors. All rights reserved.
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
using BH.oM.Reflection.Attributes;
using BH.Engine.Serialiser;
using BH.oM.Base;
using BH.Engine.Reflection;
using Microsoft.CSharp.RuntimeBinder;
using UT = BH.oM.Test.UnitTests;
using BH.oM.Reflection.Interface;
using BH.oM.Reflection;

namespace BH.Engine.UnitTest
{
    public static partial class Compute
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("Executes a unit test and compares the result of running the method with provided test data with the corresponding expected outputs.")]
        [Input("test", "The test to run, containing the method to be executed as well as the data to test on.")]
        [MultiOutput(0, "runErrors", "Any errors encountered during the execution of the method.")]
        [MultiOutput(1, "comparisonErrors", "Any errors encountered during the comparison of the expected output data with the result of running the method.")]
        public static Output<List<string>, List<string>> CheckTest(this UT.UnitTest test)
        {

            List<List<object>> results = new List<List<object>>();
            List<string> runErrors = new List<string>();
            List<string> comparisonErrors = new List<string>();
            MethodBase method = test.Method;
            foreach (UT.TestData data in test.Data)
            {
                var result = Run(method, data);

                if (result.Item2.Count != 0)
                    runErrors.AddRange(result.Item2);
                else
                {
                    try
                    {
                        if (result.Item1.Count != data.Outputs.Count)
                        {
                            comparisonErrors.Add("Running of the method returned a different number of results compared to the expected output.");
                            continue;
                        }

                        for (int i = 0; i < result.Item1.Count; i++)
                        {
                            object resultObj = result.Item1[i];
                            object refObject = CastToType(data.Outputs[i], resultObj.GetType());

                            if(!Engine.Test.Query.IsEqual(resultObj, refObject))
                                comparisonErrors.Add("Didn't return the correct output for output " + i);
                        }

                    }
                    catch
                    {
                        comparisonErrors.Add("Failed to run the comparison");
                    }
                }

            }
            return new Output<List<string>, List<string>> { Item1 = runErrors, Item2 = comparisonErrors };
        }
    

        /***************************************************/
    }
}