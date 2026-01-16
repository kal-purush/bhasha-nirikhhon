using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Class_Scheduler.Tests.ParseTests
{
    [TestClass]
    public class TestStaff
    {
        [TestMethod]
        public void TestStaffOutput()
        {
            //Replace this filename with what file you want to test.
            List<String> fileContents = FileReading.readFile("tests/staff.json");

            FileParser fileParser = new FileParser();

            //Checks whether the data is staff or unit.
            String dataType = fileParser.checkDataType(fileContents);

            //Parse the data accordingly, can change to a switch case later for performance.
            if (dataType.Equals("staffData"))
            {
                List<Staff> staffList = fileParser.parseStaffData(fileContents);

                foreach (Staff currentStaff in staffList)
                {
                    Console.WriteLine(currentStaff.ToString() + "\n");
                }
            }
            else
            {
                //Add a custom exception here later.
            }
        }
    }
}