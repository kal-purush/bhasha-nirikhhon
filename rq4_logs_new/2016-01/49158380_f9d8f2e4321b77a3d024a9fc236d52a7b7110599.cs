using Rock.Plugin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace com.reallifeministries.RockExtensions.Migrations
{
    class _007_DefinedTypesForTextAttendance : Migration
    {
        public override void Down()
        {
            RockMigrationHelper.DeleteAttribute("17B0E99E-2D10-442A-9342-1F9F07996C2B"); // AttendanceGroup
            RockMigrationHelper.DeleteAttribute("3A526D6C-06FC-46CD-A447-9A6D9A74BB4F"); // KeywordExpression
            RockMigrationHelper.DeleteAttribute("67E09C64-3558-48B7-9A27-A9499D0826E8"); // WorkflowNameTemplate
            RockMigrationHelper.DeleteAttribute("79E3B97B-A717-45AB-A279-335FCDEA141A"); // Campus
            RockMigrationHelper.DeleteAttribute("0097D00F-1F29-4217-8E67-D37A619A6FA3"); // WorkflowType
            RockMigrationHelper.DeleteAttribute("836CFC0B-6750-4A93-8309-EAB868B845AF"); // WorkflowAttributes

            RockMigrationHelper.DeleteDefinedValue("2294DC3F-8539-4442-B30E-36D647C70261"); // +12082976885
            RockMigrationHelper.DeleteDefinedValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74"); // +12082976885
            RockMigrationHelper.DeleteDefinedValue("BA83A4C4-1684-4755-BEEF-CE21E89A761B"); // +12082976885
            RockMigrationHelper.DeleteDefinedValue("C9C34BD5-D286-4002-AC28-474C2D6DFAED"); // +12082976885
            RockMigrationHelper.DeleteDefinedValue("F3AE9577-2121-4971-BBED-CEBAB6AAF624"); // +12082976885
            RockMigrationHelper.DeleteDefinedValue("FE137A19-ECCE-44B5-9643-00C056CF2A72"); // +12082976885
            RockMigrationHelper.DeleteDefinedType("2CACB86F-D811-4483-98E1-272F1FF8FF5D"); // Text To Workflow
        }

        public override void Up()
        {
            RockMigrationHelper.AddDefinedType("Workflow", "Text To Workflow", "Matches SMS phones and keywords to launch workflows of various types", "2CACB86F-D811-4483-98E1-272F1FF8FF5D", @"

                The following merge fields are available for both the ''Name Template'' and ''Workflow Attributes'' attributes.
                <p>
                    <a data-toggle=""collapse""  href=""#collapsefields"" class=''btn btn-action btn-xs''>show/hide fields</a>
                </p>

                <div id=""collapsefields"" class=""panel-collapse collapse"">
                <pre>
                {
                   ""FromPhone"":""+15555555555"",
                   ""ToPhone"":""+15555555555"",
                   ""MessageBody"":""keyword"",
                   ""ReceivedTime"":""10:02 PM"",
                   ""ReceivedDate"":""7/29/2014"",
                   ""FromPerson"":{
                      ""FullName"":""Ted Decker"",
                      ""IsDeceased"":false,
                      ""TitleValueId"":null,
                      ""FirstName"":""Theodore"",
                      ""NickName"":""Ted"",
                      ""MiddleName"":"""",
                      ""LastName"":""Decker"",
                      ""SuffixValueId"":null,
                      ""PhotoId"":36,
                      ""BirthDay"":10,
                      ""BirthMonth"":2,
                      ""BirthYear"":1976,
                      ""Gender"":1,
                      ""MaritalStatusValueId"":143,
                      ""AnniversaryDate"":null,
                      ""GraduationDate"":""1994-06-01T00:00:00"",
                      ""GivingGroupId"":41,
                      ""Email"":""ted@rocksoliddemochurch.com"",
                      ""IsEmailActive"":true,
                      ""EmailNote"":null,
                      ""EmailPreference"":0,
                      ""ReviewReasonNote"":null,
                      ""InactiveReasonNote"":null,
                      ""SystemNote"":null,
                      ""ViewedCount"":null,
                      ""PrimaryAliasId"":2,
                      ""BirthdayDayOfWeek"":""Monday"",
                      ""BirthdayDayOfWeekShort"":""Mon"",
                      ""PhotoUrl"":""/GetImage.ashx?id=36"",
                      ""BirthDate"":""1976-02-10T00:00:00"",
                      ""Age"":38,
                      ""DaysToBirthday"":196,
                      ""Grade"":33,
                      ""GradeFormatted"":"""",
                      ""CreatedDateTime"":null,
                      ""ModifiedDateTime"":""2014-07-23T23:26:18.357"",
                      ""CreatedByPersonAliasId"":null,
                      ""ModifiedByPersonAliasId"":1,
                      ""Attributes"":null,
                      ""AttributeValues"":null,
                      ""Id"":2,
                      ""Guid"":""8fedc6ee-8630-41ed-9fc5-c7157fd1eaa4"",
                      ""UrlEncodedKey"":""EAAAAHkbB7e!2bYK0Xdq9Ib9ePIpblOpW9jAghYRMyMWe9vjb3BF8mvzaL6CCNVZrs6zk4nNCgX9JkXkmY3KRudX!2bKO!2fg!3d"",
	                  ""ConnectionStatusValue"":{
                         ""Order"":0,
                         ""Name"":""Member"",
                         ""Description"":""Applied to individuals who have completed all requirements established to become a member."",
                         ""Id"":65,
                         ""Guid"":""41540783-d9ef-4c70-8f1d-c9e83d91ed5f"",
                      },
                      ""MaritalStatusValue"":{
                         ""Order"":0,
                         ""Name"":""Married"",
                         ""Description"":""Used with an individual is married."",
                         ""Id"":143,
                         ""Guid"":""5fe5a540-7d9f-433e-b47e-4229d1472248"",
                         ""UrlEncodedKey"":""EAAAAHiZvJY3Dkl85B1F8SJ2AnS8onTRarYspmUq5VOIkKRWurg4E913MdwkRq2tzQWF7qQoraHlMey24opvgDMvNQ8!3d""
                      },
                      ""PhoneNumbers"":[
                         {
                            ""NumberTypeValue"":{
                               ""Order"":1,
                               ""Name"":""Home"",
                               ""Description"":""Home phone number"",
                               ""Id"":13,
                               ""Guid"":""aa8732fb-2cea-4c76-8d6d-6aaa2c6a4303"",
                               ""UrlEncodedKey"":""EAAAABRiGGLD6CTiCKqb29OBr2KkOwimKb0BQp8oKV0gvKIzSAwNzMXg!2bwvQUHvY!2brdPIJZ7nBdJ1QcwbUsUwB7ITYc!3d""
                            },
                            ""CountryCode"":""1"",
                            ""Number"":""6235553322"",
                            ""NumberFormatted"":""(623) 555-3322"",
                            ""Extension"":null,
                            ""NumberTypeValueId"":13,
                            ""IsMessagingEnabled"":false,
                            ""IsUnlisted"":false,
                            ""Description"":null,
                            ""NumberFormattedWithCountryCode"":""+1 (623) 555-3322"",
                            ""Id"":1,
                            ""Guid"":""fdfbd202-a67e-4ea5-9cff-2b939593d054"",
                            ""UrlEncodedKey"":""EAAAALquQ6scz74JCg3iV50!2bnF0LIDQHyjyJOYxwRn7BSbFX4Z6Y76g7eBbFoXpkSQ67FjHd8VZ2M4!2bZzzbAHhKLYtA!3d""
                         },
                         {
                            ""NumberTypeValue"":{
                               ""Order"":2,
                               ""Name"":""Work"",
                               ""Description"":""Work phone number"",
                               ""Guid"":""2cc66d5a-f61c-4b74-9af9-590a9847c13c"",
                               ""UrlEncodedKey"":""EAAAAAXigHXhetFtBvzV!2bFC7PbJC!2bYcv77nHDYssZobfHDgyvd004RG7xxctsdccwZIHFajKAcfzgw2mwH7IS8iCJ8Y!3d""
                            },
                            ""CountryCode"":""1"",
                            ""Number"":""6235552444"",
                            ""NumberFormatted"":""(623) 555-2444"",
                            ""Extension"":null,
                            ""NumberTypeValueId"":136,
                            ""IsMessagingEnabled"":false,
                            ""IsUnlisted"":false,
                            ""Description"":null,
                            ""NumberFormattedWithCountryCode"":""+1 (623) 555-2444"",
                            ""Id"":3,
                            ""Guid"":""e26f602c-a742-4ee5-b332-5583b2bd31c0"",
                            ""UrlEncodedKey"":""EAAAADItUMe9!2bgCRZ3uugX9STAPz7fllpXVUFdK65R5DlIMYjyjCFDwQIJ7rp3tZrztTsAMsRk5AbuocEH3TvXY3Cgk!3d""
                         },
                         {
                            ""NumberTypeValue"":{
                               ""Order"":0,
                               ""Name"":""Mobile"",
                               ""Description"":""Mobile/Cell phone number"",
                               ""Id"":12,
                               ""Guid"":""407e7e45-7b2e-4fcd-9605-ecb1339f2453"",
                               ""UrlEncodedKey"":""EAAAANaN5diFoMocUiXgsoPb7g!2fwZ0NA8lvD8HPIevtFueL1YrEBGCM9GQF4FANqYrid4yQZm5nFUR!2bjt9JfpMf12Rg!3d""
                            },
                            ""CountryCode"":""1"",
                            ""Number"":""6238662792"",
                            ""NumberFormatted"":""(623) 866-2792"",
                            ""Extension"":null,
                            ""NumberTypeValueId"":12,
                            ""IsMessagingEnabled"":true,
                            ""IsUnlisted"":false,
                            ""Description"":null,
                            ""NumberFormattedWithCountryCode"":""+1 (623) 866-2792"",
                            ""Id"":15,
                            ""Guid"":""96e6b17e-7a18-4231-915a-27a98c02d4c4"",
                            ""UrlEncodedKey"":""EAAAADHSjNg82gKW!2bII0kmO3Wd3bKBOAOJjW8Mb!2buKiNGhzr4ElEd4G4PGz7YHoRWqb4ozsiOr3A7mhzJw0VZg!2fD7RY!3d""
                         }
                      ],
                      ""Photo"":{
                         ""BinaryFileType"":{
                            ""Name"":""Person Image"",
                            ""Description"":""Image of a Person"",
                            ""IconCssClass"":""fa fa-camera"",
	                    },
                         ""IsTemporary"":false,
                         ""BinaryFileTypeId"":5,
                         ""Url"":null,
                         ""FileName"":""decker_ted.jpg"",
                         ""MimeType"":""image/jpeg"",
                         ""Description"":null,
                         ""StorageEntityTypeId"":51,
                         ""CreatedDateTime"":null,
                         ""ModifiedDateTime"":null,
                         ""CreatedByPersonAliasId"":null,
                         ""ModifiedByPersonAliasId"":null,
                         ""Attributes"":null,
                         ""AttributeValues"":null,
                         ""Id"":36,
                         ""Guid"":""5c875f30-7e0e-4d2f-a14b-51aa01e1d887"",
                         ""UrlEncodedKey"":""EAAAAGGE!2budoDA82FsBBemJhMyckBepZzs6zblCc0RNJ1j!2bjtnP!2bbzTTGDffFM7brwVOaM!2f6i0Aa2iyc6U1DyorqpIw!3d""
                      },
                      ""RecordStatusReasonValue"":null,
                      ""RecordStatusValue"":{
                         ""Order"":0,
                         ""Name"":""Active"",
                         ""Description"":""Denotes an individual that is actively participating in the activities or services of the organization."",
                         ""Id"":3,
                         ""Guid"":""618f906c-c33d-4fa3-8aef-e58cb7b63f1e"",
                         ""UrlEncodedKey"":""EAAAABP8y!2bgB9EHvyeR2Tr7miJ9SXQRqyr7lNsl98PUWXOqXclMLzmnQVRm8Msmz0!2f1FwMsTsNjPLz6t!2fy1GRuB!2fNl4!3d""
                      },
                      ""RecordTypeValue"":{
                         ""Order"":0,
                         ""Name"":""Person"",
                         ""Description"":""A Person Record"",
                         ""Id"":1,
                         ""Guid"":""36cf10d6-c695-413d-8e7c-4546efef385e"",
                         ""UrlEncodedKey"":""EAAAAPmuNdsg7fGQlwQdnyA7NVA3cihjY2nm9crF18A629Vz33FJ8p7SARghTWQ8YJ2D!2fY5g8xsLCk6ImNq3UUczuno!3d""
                      },
                      ""ReviewReasonValue"":null,
                      ""SuffixValue"":null,
                      ""TitleValue"":null,
                   }
                }
                </pre>


                </div>
                ");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "1B71FEF4-201F-4D53-8C60-2DF21F1985ED", "Campus", "Campus", "campus to post attendance to", 101, "76882ae3-1ce8-42a6-a2b6-8c0b29cf8cf8", "79E3B97B-A717-45AB-A279-335FCDEA141A");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "46A03F59-55D3-4ACE-ADD5-B4642225DD20", "Text Attendance Workflow", "WorkflowType", "The type of workflow to launch.", 3, "", "0097D00F-1F29-4217-8E67-D37A619A6FA3");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "73B02051-0D38-4AD9-BF81-A2D477DE4F70", "Workflow Attributes", "WorkflowAttributes", "Key/value list of workflow attributes to set with the given lava merge template. See the defined type’s help text for a listing of merge fields. <span class='tip tip-lava'></span>", 5, "", "836CFC0B-6750-4A93-8309-EAB868B845AF");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "9C204CD0-1233-41C5-818A-C5DA439445AA", "Workflow Name Template", "WorkflowNameTemplate", "The lava template to use for setting the workflow name. See the defined type's help text for a listing of merge fields. <span class='tip tip-lava'></span>", 4, "", "67E09C64-3558-48B7-9A27-A9499D0826E8");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "A75DFC58-7A1B-4799-BF31-451B2BBE38FF", "Keyword Expression", "KeywordExpression", "", 2, "", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F");
            RockMigrationHelper.AddDefinedTypeAttribute("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "F4399CEF-827B-48B2-A735-F7806FCFE8E8", "AttendanceGroup", "AttendanceGroup", "The attendance group", 102, "7105fcd2-3b66-4b6c-9ab6-4ce5fd572a1e", "17B0E99E-2D10-442A-9342-1F9F07996C2B");

            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "PF - Text Attendance Option (3) - Join a Homegroup", "C9C34BD5-D286-4002-AC28-474C2D6DFAED", false);
            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "PF: Text Attendance Option (2) - Prayer Request", "2294DC3F-8539-4442-B30E-36D647C70261", false);
            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "PF: Text Attendance Option (4) - Review Contact Info", "FE137A19-ECCE-44B5-9643-00C056CF2A72", false);
            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "PF: Text Attendance Option (5) - Remove Last Attendance", "F3AE9577-2121-4971-BBED-CEBAB6AAF624", false);
            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "PF: Text in Attendance Option (1) - Check in Household", "BA83A4C4-1684-4755-BEEF-CE21E89A761B", false);
            RockMigrationHelper.AddDefinedValue("2CACB86F-D811-4483-98E1-272F1FF8FF5D", "+12082976885", "Post Falls WS - lifer####", "B4016741-F6B0-4E53-B0FB-BE05A8272F74", false);

            RockMigrationHelper.AddDefinedValueAttributeValue("2294DC3F-8539-4442-B30E-36D647C70261", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"88978527-0ca4-4c52-8fda-cf915fafa472");
            RockMigrationHelper.AddDefinedValueAttributeValue("2294DC3F-8539-4442-B30E-36D647C70261", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"2.*");
            RockMigrationHelper.AddDefinedValueAttributeValue("2294DC3F-8539-4442-B30E-36D647C70261", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"");
            RockMigrationHelper.AddDefinedValueAttributeValue("2294DC3F-8539-4442-B30E-36D647C70261", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPhone^{{FromPhone}}|ReceivedDateTime^{{ReceivedDateTime}}|MessageBody^{{MessageBody}}|");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"d8354ea7-7da9-449d-94ee-210c6b9496d8");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "17B0E99E-2D10-442A-9342-1F9F07996C2B", @"7105fcd2-3b66-4b6c-9ab6-4ce5fd572a1e");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"(?i)lifer[0-9]...");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"PF Message from: {{ FromPerson.FullName }}");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "79E3B97B-A717-45AB-A279-335FCDEA141A", @"76882ae3-1ce8-42a6-a2b6-8c0b29cf8cf8");
            RockMigrationHelper.AddDefinedValueAttributeValue("B4016741-F6B0-4E53-B0FB-BE05A8272F74", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPhone^{{ FromPhone }}|ReceivedDate^{{ ReceivedDate }}|ReceivedTime^{{ ReceivedTime }}|MessageBody^{{ MessageBody }}|");
            RockMigrationHelper.AddDefinedValueAttributeValue("BA83A4C4-1684-4755-BEEF-CE21E89A761B", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"c5fe671a-c3ca-4785-95dd-3cb10f4308b0");
            RockMigrationHelper.AddDefinedValueAttributeValue("BA83A4C4-1684-4755-BEEF-CE21E89A761B", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"1.*");
            RockMigrationHelper.AddDefinedValueAttributeValue("BA83A4C4-1684-4755-BEEF-CE21E89A761B", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"");
            RockMigrationHelper.AddDefinedValueAttributeValue("BA83A4C4-1684-4755-BEEF-CE21E89A761B", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPerson^{{FromPerson}}|FromPhone^{{FromPhone}}|ReceivedDateTime^{{ReceivedDateTime}}|");
            RockMigrationHelper.AddDefinedValueAttributeValue("C9C34BD5-D286-4002-AC28-474C2D6DFAED", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"ca876138-0461-40ad-ab4f-67678b8baeca");
            RockMigrationHelper.AddDefinedValueAttributeValue("C9C34BD5-D286-4002-AC28-474C2D6DFAED", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"3.*");
            RockMigrationHelper.AddDefinedValueAttributeValue("C9C34BD5-D286-4002-AC28-474C2D6DFAED", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"");
            RockMigrationHelper.AddDefinedValueAttributeValue("C9C34BD5-D286-4002-AC28-474C2D6DFAED", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPhone^{{FromPhone}}|ReceivedDateTime^{{ReceivedDateTime}}|MessageBody^{{MessageBody}}|");
            RockMigrationHelper.AddDefinedValueAttributeValue("F3AE9577-2121-4971-BBED-CEBAB6AAF624", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"a74396b8-4723-4be7-9f4a-14506c921fc8");
            RockMigrationHelper.AddDefinedValueAttributeValue("F3AE9577-2121-4971-BBED-CEBAB6AAF624", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"5.*");
            RockMigrationHelper.AddDefinedValueAttributeValue("F3AE9577-2121-4971-BBED-CEBAB6AAF624", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"");
            RockMigrationHelper.AddDefinedValueAttributeValue("F3AE9577-2121-4971-BBED-CEBAB6AAF624", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPhone^{{FromPhone}}|ReceivedDateTime^{{ReceivedDateTime}}|MessageBody^{{MessageBody}}|");
            RockMigrationHelper.AddDefinedValueAttributeValue("FE137A19-ECCE-44B5-9643-00C056CF2A72", "0097D00F-1F29-4217-8E67-D37A619A6FA3", @"feb34437-9600-4330-855f-85404ec10018");
            RockMigrationHelper.AddDefinedValueAttributeValue("FE137A19-ECCE-44B5-9643-00C056CF2A72", "3A526D6C-06FC-46CD-A447-9A6D9A74BB4F", @"4.*");
            RockMigrationHelper.AddDefinedValueAttributeValue("FE137A19-ECCE-44B5-9643-00C056CF2A72", "67E09C64-3558-48B7-9A27-A9499D0826E8", @"");
            RockMigrationHelper.AddDefinedValueAttributeValue("FE137A19-ECCE-44B5-9643-00C056CF2A72", "836CFC0B-6750-4A93-8309-EAB868B845AF", @"FromPhone^{{FromPhone}}|ReceivedDateTime^{{ReceivedDateTime}}|MessageBody^{{MessageBody}}|");
        }
    }
}