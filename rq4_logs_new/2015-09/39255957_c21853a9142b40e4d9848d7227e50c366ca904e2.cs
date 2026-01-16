using System;
using System.Collections.Generic;

namespace NLZS.Scheduling.Models
{
    public class AvailabilityMatrix
    {
        readonly Dictionary<int, List<Employee>> _matrix = new Dictionary<int, List<Employee>>();
        
        public AvailabilityMatrix(IEnumerable<Employee> employees)
        {
            foreach (var employee in employees)
            {
                foreach (var timeslot in employee.Availability)
                {
                    AddAvailability(timeslot, employee);
                }
            }
        }

        public IEnumerable<Employee> GetAvailableEmployees(int timeslot)
        {
            if (_matrix.ContainsKey(timeslot))
            {
                return _matrix[timeslot];
            }
            throw new Exception("Timeslot not found");
        } 

        public bool IsAvailable(int timeslot, Employee employee)
        {
            if (!_matrix.ContainsKey(timeslot))
            {
                return false;
            }

            return _matrix[timeslot].Exists(x => x.Name == employee.Name);
        }

        public void AddAvailability(int timeslot, Employee employee)
        {
            if(!_matrix.ContainsKey(timeslot)){
                _matrix.Add(timeslot, new List<Employee>());
            }

            _matrix[timeslot].Add(employee);
        }

        public void RemoveAvailability(int timeslot, Employee employee)
        {
            _matrix[timeslot].Remove(employee);
        }
    }
}