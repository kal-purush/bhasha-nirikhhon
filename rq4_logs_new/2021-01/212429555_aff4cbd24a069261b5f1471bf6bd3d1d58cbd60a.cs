/*
 * This file is part of the Buildings and Habitats object Model (BHoM)
 * Copyright (c) 2015 - 2021, the respective contributors. All rights reserved.
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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BH.oM.Test;
using System.ComponentModel;

namespace BH.oM.UnitTest.Results
{
    public class ComparisonDifference : ITestInformation
    {
        [Description("Human readable message describing the information.")]
        public virtual string Message { get; set; } = "";

        [Description("Status describing")]
        public virtual TestStatus Status { get; set; } = TestStatus.Error;

        [Description("Provides the UTC time of when the Test Result was executed.")]
        public virtual DateTime UTCTime { get; set; } = DateTime.UtcNow;

        [Description("The name of the property that did not return as equal to the reference value.")]
        public virtual string Property { get; set; } = "";

        [Description("Value of the property in the expected output of the UnitTest executed.")]
        public virtual object ReferenceValue { get; set; }

        [Description("Value of the property returned by running the method.")]
        public virtual object RunValue { get; set; }
    }
}