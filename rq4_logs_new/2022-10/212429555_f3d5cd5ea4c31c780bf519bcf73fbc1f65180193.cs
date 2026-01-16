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

using BH.oM.Base;
using BH.oM.Base.Attributes;
using BH.oM.Data.Library;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.IO;

namespace BH.Engine.UnitTest
{
    public static partial class Query
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("Gets a DataSet from a filepath.")]
        [Input("fileName", "Full filepath to the dataset.")]
        [Output("dataSet", "The DataSet contained in the file.")]
        public static Dataset DatasetFromFile(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                Base.Compute.RecordError("No filename provided.");
                return null;
            }
            StreamReader sr = new StreamReader(fileName);
            string line = sr.ReadToEnd();
            sr.Close();

            object ds = Serialiser.Convert.FromJson(line);
            if (ds == null)
            {
                Base.Compute.RecordError("Dataset did not deserialise correctly.");
                return null;
            }

            Dataset testSet = ds as Dataset;
            if (testSet == null)
            {
                Base.Compute.RecordError("Dataset did not deserialise correctly as a BHoM Dataset.");
                return null;
            }

            return testSet;
        }

        /***************************************************/
    }
}