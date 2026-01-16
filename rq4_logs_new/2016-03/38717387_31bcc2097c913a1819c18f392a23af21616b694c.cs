namespace Mail2BugUnitTests.WorkItemManagement
{
    using System.Collections.Generic;
    using System.IO;
    using System.Xml;

    using Microsoft.AzureAd.Icm.Types;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Mail2Bug;
    using Mail2Bug.WorkItemManagement;

    [TestClass]
    public class IcmWorkItemManagerTest
    {
        [TestMethod]
        public void GenerateDescriptionEntry()
        {
            Dictionary<string, string> values = new Dictionary<string, string>
            {
                { FieldNames.Incident.Description, "" },
                { FieldNames.Incident.CreatedBy, "" },
                { FieldNames.Incident.CreateDate, "" }
            };

            DescriptionEntry result = IcmWorkItemManagment.GenerateDescriptionEntry(values, -1);
            Assert.Inconclusive();
        }

        #region TruncateXml Tests

        // Output file names for debugging test failures.
        private const string OutputFileNameA = "XmlInput.html";
        private const string OutputFileNameB = "XmlResult.html";

        private const string InsertedMessage = "<div>" + IcmWorkItemManagment.TruncationMessage + "</div>";
        private const string SampleXml_Small =     @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th></tr></tbody></table></div></div></div></span>";
        private const string ExpectedXml_Small00 = @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th></th></tr></tbody></table></div></div></div>" + InsertedMessage + "</span>";
        private const string ExpectedXml_Small01 = @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th></tr></tbody></table></div></div></div>" + InsertedMessage + "</span>";

        private const string SampleXml_Large = @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>PassPercentage</th><th>TotalFailed</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>PollReport</td><td><span>Success</span></td><td>99.53%</td><td>403</td><td>86221</td><td>99%</td><td> </td><td><a href=""http://elasticsearch-dvs:88/#/dashboard/ExceptionDashboardProd?_g=(refreshInterval:(display:Off,section:0,value:0),time:(from:'2016-03-17T18:47:30.484Z',mode:absolute,to:'2016-03-17T19:07:30.484Z'))&amp;_a=(filters:!((meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errorcategory,negate:!t,value:Security),query:(match:(result.errorcategory:(query:Security,type:phrase)))),(meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errorcategory,negate:!t,value:Validation),query:(match:(result.errorcategory:(query:Validation,type:phrase)))),(meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errormessage,negate:!t,value:'ReportListFilter has no values'),query:(match:(result.errormessage:(query:'ReportListFilter has no values',type:phrase))))),panels:!((col:1,id:ErrorsOverTimeLineChartProd,row:1,size_x:12,size_y:3,type:visualization),(col:1,id:ErrorsByResultCodeBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:10,id:ErrorsByOperationBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:7,id:ErrorsByServiceBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:4,id:ErrorsByServerBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:1,id:TopLevelErrorSearchProd,row:11,size_x:12,size_y:7,type:search),(col:1,id:ErrorsByServerOverTimeLineChartProd,row:4,size_x:12,size_y:3,type:visualization)),query:(query_string:(analyze_wildcard:!t,query:'service:IMiddleTier AND operation:PollReport')),title:ExceptionDashboardProd)"" target=""_blank"">Detail</a></td></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>SubmitReport</td><td><span>Error</span></td><td>98.97%</td><td>494</td><td>47766</td><td>99%</td><td>PassRatio below the alert threshold:99%</td><td><a href=""http://elasticsearch-dvs:88/#/dashboard/ExceptionDashboardProdod)"" target=""_blank"">Detail</a></td></tr></tbody></table></div><div><table><tbody><tr><td colspan=""9"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>AvgLatency</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Performance</td><td>IMiddleTier</td><td>PollReport</td><td><span>Error</span></td><td>325 ms</td><td>81986</td><td>250 ms</td><td>Latency exceed the alert threshold:250 ms</td><td><a href=""http://elasticsearch-dvs:88/#/visualizeline))"" target=""_blank"">Detail</a></td></tr></tbody></table></div><div><table><tbody></tbody></table></div><div><table><tbody><tr><td colspan=""7"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>TotalRequests</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Elasticsearch Health</td><td>Elasticsearch</td><td>Any documents received</td><td><span>Success</span></td><td>214769</td><td> </td><td><a href=""http://elasticsearch-dvs:81/_plugin/kopf/"" target=""_blank"">Detail</a></td></tr><tr><td>Elasticsearch Health</td><td>Elasticsearch</td><td>Color</td><td><span>Success</span></td><td>0</td><td>UnassignedShards:0, InitializingShards:0, RelocatingShards:0, ActiveShards:184</td><td><a href=""http://elasticsearch-dvs:81/_plugin/kopf/"" target=""_blank"">Detail</a></td></tr></tbody></table></div></div></div></span>";
        private const string ExpectedXml_Large00 = @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>PassPercentage</th><th>TotalFailed</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>PollReport</td><td><span>Success</span></td><td>99.53%</td><td>403</td><td>86221</td><td>99%</td><td> </td><td><a href=""http://elasticsearch-dvs:88/#/dashboard/ExceptionDashboardProd?_g=(refreshInterval:(display:Off,section:0,value:0),time:(from:'2016-03-17T18:47:30.484Z',mode:absolute,to:'2016-03-17T19:07:30.484Z'))&amp;_a=(filters:!((meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errorcategory,negate:!t,value:Security),query:(match:(result.errorcategory:(query:Security,type:phrase)))),(meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errorcategory,negate:!t,value:Validation),query:(match:(result.errorcategory:(query:Validation,type:phrase)))),(meta:(disabled:!f,index:[dvs-optics-]YYYY.MM.DD,key:result.errormessage,negate:!t,value:'ReportListFilter has no values'),query:(match:(result.errormessage:(query:'ReportListFilter has no values',type:phrase))))),panels:!((col:1,id:ErrorsOverTimeLineChartProd,row:1,size_x:12,size_y:3,type:visualization),(col:1,id:ErrorsByResultCodeBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:10,id:ErrorsByOperationBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:7,id:ErrorsByServiceBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:4,id:ErrorsByServerBarChartProd,row:7,size_x:3,size_y:4,type:visualization),(col:1,id:TopLevelErrorSearchProd,row:11,size_x:12,size_y:7,type:search),(col:1,id:ErrorsByServerOverTimeLineChartProd,row:4,size_x:12,size_y:3,type:visualization)),query:(query_string:(analyze_wildcard:!t,query:'service:IMiddleTier AND operation:PollReport')),title:ExceptionDashboardProd)"" target=""_blank"">Detail</a></td></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>SubmitReport</td><td><span>Error</span></td><td>98.97%</td><td>494</td><td>47766</td><td>99%</td><td>PassRatio below the alert threshold:99%</td><td><a href=""http://elasticsearch-dvs:88/#/dashboard/ExceptionDashboardProdod)"" target=""_blank"">Detail</a></td></tr></tbody></table></div><div><table><tbody><tr><td colspan=""9"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>AvgLatency</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Performance</td><td>IMiddleTier</td><td>PollReport</td><td><span>Error</span></td><td>325 ms</td><td>81986</td><td>250 ms</td></tr></tbody></table></div></div></div>" + InsertedMessage + "</span>";
        private const string ExpectedXml_Large01 = @"<span><div><div /><div><div><table><tbody><tr><td colspan=""10"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>PassPercentage</th><th>TotalFailed</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>PollReport</td><td><span>Success</span></td><td>99.53%</td><td>403</td><td>86221</td><td>99%</td><td> </td><td><div>** Mail2IcM removed hyperlink **</div></td></tr><tr><td>Service Availability</td><td>IMiddleTier</td><td>SubmitReport</td><td><span>Error</span></td><td>98.97%</td><td>494</td><td>47766</td><td>99%</td><td>PassRatio below the alert threshold:99%</td><td><a href=""http://elasticsearch-dvs:88/#/dashboard/ExceptionDashboardProdod)"" target=""_blank"">Detail</a></td></tr></tbody></table></div><div><table><tbody><tr><td colspan=""9"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>AvgLatency</th><th>TotalRequests</th><th>Threshold</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Service Performance</td><td>IMiddleTier</td><td>PollReport</td><td><span>Error</span></td><td>325 ms</td><td>81986</td><td>250 ms</td><td>Latency exceed the alert threshold:250 ms</td><td><a href=""http://elasticsearch-dvs:88/#/visualizeline))"" target=""_blank"">Detail</a></td></tr></tbody></table></div><div><table><tbody></tbody></table></div><div><table><tbody><tr><td colspan=""7"">List</td></tr><tr><th>Category</th><th>Service</th><th>Operation</th><th>Status</th><th>TotalRequests</th><th>Message</th><th>InvestigationLink</th></tr><tr><td>Elasticsearch Health</td><td>Elasticsearch</td><td>Any documents received</td><td><span>Success</span></td><td>214769</td><td> </td><td><a href=""http://elasticsearch-dvs:81/_plugin/kopf/"" target=""_blank"">Detail</a></td></tr><tr><td>Elasticsearch Health</td><td>Elasticsearch</td><td>Color</td><td><span>Success</span></td><td>0</td><td>UnassignedShards:0, InitializingShards:0, RelocatingShards:0, ActiveShards:184</td><td><a href=""http://elasticsearch-dvs:81/_plugin/kopf/"" target=""_blank"">Detail</a></td></tr></tbody></table></div></div></div>" + InsertedMessage + "</span>";

        [TestMethod]
        public void TruncateXml_NoChange()
        {
            int maximumLength = SampleXml_Large.Length;
            string result = IcmWorkItemManagment.TruncateXml(SampleXml_Large, maximumLength, -1);
            Assert.AreEqual(SampleXml_Large, result);
        }

        [TestMethod]
        public void TruncateXml_OneCharShorter()
        {
            int maximumLength = SampleXml_Small.Length - 1;
            string result = IcmWorkItemManagment.TruncateXml(SampleXml_Small, maximumLength, -1);

            // File output for comparison when debugging failure.
            //WriteStringAsFormattedXml(ExpectedXml_Small00, OutputFileNameA);
            //WriteStringAsFormattedXml(result, OutputFileNameB);

            Assert.AreEqual(ExpectedXml_Small00, result);
        }

        [TestMethod]
        public void TruncateXml_DropSiblings()
        {
            int maximumLength = SampleXml_Small.Length - 45;
            string result = IcmWorkItemManagment.TruncateXml(SampleXml_Small, maximumLength, -1);

            // File output for comparison when debugging failure.
            //WriteStringAsFormattedXml(SampleXml_Small, OutputFileNameA);
            //WriteStringAsFormattedXml(result, OutputFileNameB);

            Assert.AreEqual(ExpectedXml_Small01, result);
        }

        [TestMethod]
        public void TruncateXml_LargeInput()
        {
            int maximumLength = (int) (SampleXml_Large.Length * 0.75);
            string result = IcmWorkItemManagment.TruncateXml(SampleXml_Large, maximumLength, -1);

            // File output for comparison when debugging failure.
            //WriteStringAsFormattedXml(SampleXml_Large, OutputFileNameA);
            //WriteStringAsFormattedXml(result, OutputFileNameB);

            Assert.AreEqual(ExpectedXml_Large00, result);
        }

        [TestMethod]
        public void TruncateXml_RemoveHyperlink()
        {
            int maximumLength = SampleXml_Large.Length;
            string result = IcmWorkItemManagment.TruncateXml(SampleXml_Large, maximumLength, 70);

            // File output for comparison when debugging failure.
            //WriteStringAsFormattedXml(ExpectedXml_Large01, OutputFileNameA);
            //WriteStringAsFormattedXml(result, OutputFileNameB);

            Assert.AreEqual(ExpectedXml_Large01, result);
        }

        #endregion TruncateXml Tests

        public static void WriteStringAsFormattedXml(string input, string filename)
        {
            XmlDocument document = new XmlDocument();
            document.LoadXml(input);

            XmlWriterSettings settings = new XmlWriterSettings { Indent = true, OmitXmlDeclaration = true};
            using (XmlWriter xmlWriter = XmlWriter.Create(filename, settings))
            {
                document.WriteTo(xmlWriter);
                xmlWriter.Flush();
            }
        }
    }
}