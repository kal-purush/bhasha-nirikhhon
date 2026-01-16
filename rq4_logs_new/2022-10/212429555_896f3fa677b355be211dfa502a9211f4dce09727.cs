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
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using BH.oM.Data.Library;
using UT = BH.oM.Test.UnitTests;

namespace BH.Engine.UnitTest
{
    public static partial class Compute
    {
        /***************************************************/
        /**** Public Methods                            ****/
        /***************************************************/

        [Description("Merges two Dataset of UnitTests into one Dataset.")]
        [Input("set1", "First Dataset to merge.")]
        [Input("set2", "Second Dataset to merge.")]
        [Output("mergedDataset", "Dataset containing UnitTests and source information from both provided Datasets.")]
        public static Dataset MergeTestDataSets(Dataset set1, Dataset set2)
        {
            if (set1 == null || set2 == null)
            {
                Base.Compute.RecordError("At least one of the Datasets to merge is null. Cannot merge Datasets.");
                return null;
            }

            List<UT.UnitTest> unitTests = set1.Data.OfType<UT.UnitTest>().ToList();
            unitTests.AddRange(set2.Data.OfType<UT.UnitTest>());

            unitTests = MergeUnitTests(unitTests, true);

            return new Dataset
            {
                Name = set1.Name,
                Data = unitTests.ToList<IBHoMObject>(),
                SourceInformation = MergeSource(set1.SourceInformation, set2.SourceInformation),
                TimeOfCreation = set1.TimeOfCreation > set2.TimeOfCreation ? set1.TimeOfCreation : set2.TimeOfCreation  //Latest date
            };
        }

        /***************************************************/
        /**** Private Methods                           ****/
        /***************************************************/

        private static Source MergeSource(Source source1, Source source2)
        {
            if (source1 == null)
                return source2;

            if (source2 == null)
                return source1;

            Source mergedSource = new Source()
            {
                Name = MergeStringProperty(source1.Name, source2.Name, nameof(source1.Name)),
                Contributors = MergeStringProperty(source1.Contributors, source2.Contributors, nameof(source1.Contributors)),
                Copyright = MergeStringProperty(source1.Copyright, source2.Copyright, nameof(source1.Copyright)),
                SourceLink = MergeStringProperty(source1.SourceLink, source2.SourceLink, nameof(source1.SourceLink)),
                Version = MergeStringProperty(source1.Version, source2.Version, nameof(source1.Version)),
                Title = MergeStringProperty(source1.Title, source2.Title, nameof(source1.Title)),
                Schema = MergeStringProperty(source1.Schema, source2.Schema, nameof(source1.Schema)),
                Publisher = MergeStringProperty(source1.Publisher, source2.Publisher, nameof(source1.Publisher)),
                Location = MergeStringProperty(source1.Location, source2.Location, nameof(source1.Location)),
                Language = MergeStringProperty(source1.Language, source2.Language, nameof(source1.Language)),
                ItemReference = MergeStringProperty(source1.ItemReference, source2.ItemReference, nameof(source1.ItemReference)),
            };

            //Authors assumed to be stored separated by new line
            List<string> authors = source1.Author.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries).ToList();  //Get Author(s) from first source
            authors.AddRange(source1.Author.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries));  //Get Author(s) from second source
            
            mergedSource.Author = string.Join(Environment.NewLine, authors.Distinct()); //Set Authors to distinct entries, separated by new line

            //Set confidence to maximum of the provided sources
            //Logic being that if at least one of the test sets have been deemed to give a high confidence, then adding more test data to that particular set 
            //is not/should not make the confidence level lower, but if anything increase it.
            mergedSource.Confidence = (Confidence)Math.Max((int)source1.Confidence, (int)source2.Confidence);   

            return mergedSource;
        }

        /***************************************************/

        private static string MergeStringProperty(string source1, string source2, string propertyName)
        {
            if (string.IsNullOrWhiteSpace(source1))
                return source2;
            else
            {
                if (string.IsNullOrWhiteSpace(source2))
                    return source1;

                if (source1 != source2)
                    BH.Engine.Base.Compute.RecordWarning($"{propertyName} is diffrent for both sources. Using value from first.");

                return source1;
            }
        }

        /***************************************************/
    }
}