using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace PPSoft_SkedgeITViewModels
{
    class EmployeeViewModel : SkedgeITViewModelConfig
    {
        public int EmployeeID { get; set; }
        public string password { get; set; }
        public string firstName { get; set; }
        public string lastName { get; set; }
        public string accessLevel { get; set; }
        public DateTime SunStart { get; set; }
        public DateTime SunEnd   { get; set; }
        public DateTime MonStart { get; set; }
        public DateTime MonEnd   { get; set; }
        public DateTime TueStart { get; set; }
        public DateTime TueEnd   { get; set; }
        public DateTime WedStart { get; set; }
        public DateTime WedEnd   { get; set; }
        public DateTime ThuStart { get; set; }
        public DateTime ThuEnd   { get; set; }
        public DateTime FriStart { get; set; }
        public DateTime FriEnd   { get; set; }
        public DateTime SatStart { get; set; }
        public DateTime SatEnd   { get; set; }
        public string Message { get; set; }
        ///<summary>
        ///This Method called to Register a new Employee to the Database
        ///</summary>

        public void Register()
        {
            Dictionary<string, Object> dictionaryEmployee = new Dictionary<string,Object>();
            try
            {
                EmployeeModel myData = new EmployeeModel();
                dictionaryEmployee["password"] = password;
                dictionaryEmployee["firstName"] = firstName;
                dictionaryEmployee["lastName"] = lastName;
                dictionaryEmployee["access_levelID"] = accessLevel;
                dictionaryEmployee["sunStart"] = SunStart;
                dictionaryEmployee["sunEnd"] = SunEnd;
                dictionaryEmployee["monStart"] = MonStart;
                dictionaryEmployee["monEnd"] = MonEnd;
                dictionaryEmployee["tueStart"] = TueStart;
                dictionaryEmployee["tueEnd"] = TueEnd;
                dictionaryEmployee["wedStart"] = WedStart;
                dictionaryEmployee["wedEnd"] = WedEnd;
                dictionaryEmployee["thuStart"] = ThuStart;
                dictionaryEmployee["thuEnd"] = ThuEnd;
                dictionaryEmployee["friStart"] = FriStart;
                dictionaryEmployee["friEnd"] = FriEnd;
                dictionaryEmployee["satStart"] = SatStart;
                dictionaryEmployee["satEnd"] = SatEnd;
                EmployeeID = myData.Register(Serializer(dictionaryEmployee));
                Message = "Employee Sucessfully Registered";
            }
            catch (Exception ex)
            {
                Message = "Employee not registered, problem was " + ex.Message;
                ErrorRoutine(ex, "EmployeeViewModel", "Register");
            }
        }

        /// <summary>
        /// This will select an Employee Profile based upon provided 
        /// employee ID
        /// <accepts>int employeeID</accepts>
        /// <returns>EmployeeViewModel </returns>
        /// </summary>
        public EmployeeViewModel getEmployeeProfile(int id)
        {
            EmployeeViewModel retEmp = new EmployeeViewModel();
            SkedgeITDBEntities dbContext = new SkedgeITDBEntities();
            Employee Profile = dbContext.Employees.FirstOrDefault(e => e.employeeID == id);
            firstName = Profile.FirstName;
            lastName = Profile.LastName;
            accessLevel = Profile.AccessLevel;
            
            
            SunStart = Profile.SunStart;
            SunEnd   = Profile.SunEnd;
            MonStart = Profile.MonStart;
            MonEnd   = Profile.MonEnd;
            TueStart = Profile.TueStart;
            TueEnd   = Profile.TueEnd;
            WedStart = Profile.WedStart;
            WedEnd   = Profile.WedEnd;
            ThuStart = Profile.ThuStart;
            ThuEnd   = Profile.TueEnd;
            FriStart = Profile.FriStart;
            FriEnd   = Profile.FriEnd;
            SatStart = Profile.SatStart;
            SatEnd   = Profile.SatEnd;
            return retEmp;
        }

    }
}