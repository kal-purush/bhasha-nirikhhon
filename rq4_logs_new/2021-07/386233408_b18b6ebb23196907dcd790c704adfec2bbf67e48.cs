using Class_Scheduler.Models;
using System.Diagnostics;
using System.Collections.Generic;
using System;
using Class_Scheduler.Controllers.FileIOControllers;

public class BasicAllocator
{

    public BasicAllocator()
    {
    }
    public List<Unit> Allocate(List<Unit> units, List<Staff> staff)
    {
        List<Connection> connections = getClassConnections(units, staff);

        while (true)
        {

            connections.Sort((a, b) =>
            {
                return a.staffList.Count - b.staffList.Count;
            });

            // Remove connections with no staff.
            Console.WriteLine("Removed:");
            bool complete = true;

            foreach (Connection c in connections)
            {
                if (c.staffList.Count == 0)
                {
                    Console.WriteLine(String.Format("Allocation Complete:\n {0}", c.classInfo.ToString()));
                    continue;
                }
                complete = false;
                Class currentClass = c.classInfo;
                Staff staffMember = c.staffList[0];

                // allocate this staff member to the class
                if (currentClass.needsStaff())
                {
                    currentClass.allocatedTutors.Add(staffMember);
                    c.staffList.Remove(staffMember);
                }

            }
            if (complete)
                break;
        }
        return units;
    }

    public List<Connection> getClassConnections(List<Unit> units, List<Staff> staffList)
    {
        List<Connection> connections = new List<Connection>();
        List<Class> classes = new List<Class>();


        units.ForEach(X =>
        {
            X.classList.ForEach(Y =>
            {
                connections.Add(new Connection(Y));
            });
        });

        foreach (Connection X in connections)
        {
            TimePeriod classTime = X.classInfo.datetime;
            foreach (Staff staffMember in staffList)
            {
                // check if they can teach and if they can add them to the connections list
                if (staffMember.canTeach(classTime))
                {
                    Console.WriteLine("{0}", X.ToString());
                    X.staffList.Add(staffMember);
                }
            }
        }

        return connections;
    }
}