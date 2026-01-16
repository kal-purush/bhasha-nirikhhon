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
using System.Collections.Generic;
using System.ComponentModel;
using BH.oM.Base.Attributes;
using BH.oM.Base;
using UT = BH.oM.Test.UnitTests;
using BH.Engine.Serialiser;

namespace BH.Engine.UnitTest
{
    public static partial class Modify
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("Runs through all TestData items of the UnitTest and makes sure there are only one entry with exact matching inputs and outputs.")]
        [Input("test", "The test on which to cull the duplicate TestData.")]
        [Output("test", "Test with removed duplicate TestData entries.")]
        public static void RemoveDuplicateTestData(this UT.UnitTest test)
        {
            if (test == null)
                return;

            List<UT.TestData> uniqueData = new List<UT.TestData>();

            foreach (UT.TestData data in test.Data)
            {
                if (data == null)   //Skip null entries
                    continue;

                bool exist = false;
                for (int i = 0; i < uniqueData.Count; i++)
                {
                    if (Test.Query.IsEqual(data.Inputs, uniqueData[i].Inputs))  //Check inputs identical
                    {
                        if (Test.Query.IsEqual(data.Outputs, uniqueData[i].Outputs))    //Check outputs identical
                        {
                            exist = true;
                            if (string.IsNullOrWhiteSpace(uniqueData[i].Name) && !string.IsNullOrWhiteSpace(data.Name)) //If name is unset to already added, and set to the one compared, bring over the name
                                uniqueData[i].Name = data.Name;
                            break;
                        }
                        else
                        {
                            Base.Compute.RecordWarning("Test contains TestData with identical inputs that renders different outputs. Please check the test to ensure all entries are still valid.");
                        }
                    }
                }

                if (!exist) //No test found matching in terms of inputs and outputs
                    uniqueData.Add(data);
            }

            test.Data = uniqueData;
        }

        /***************************************************/
    }
}