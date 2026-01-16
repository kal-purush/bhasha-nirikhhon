using PDGT.Models;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace PDGT.Services
{
    public class MockService
    {
        public ObservableCollection<Exercise> Exercises { get; set; }
        public bool AvailableWeeklyQuestionnaire { get; private set; }
        public bool SessionsFinished { get; private set; }
        public int WeekNumber { get; private set; }

        public string GetWeekState()
        {

            Exercises = new ObservableCollection<Exercise>()
                {
                    new Exercise
                    {
                        Name = "Rotator cuff strenghtening I",
                        Guide = new Guide
                        {
                            Description = "Exercise guide"
                        },
                        Weight = 3,
                        Repetitions = 10,
                        Sets = 3
                    },

                    new Exercise
                    {
                        Name = "Rotator cuff strenghtening II",
                        Guide = new Guide
                        {
                            Description = "Exercise guide"
                        },
                        Weight = 3,
                        Repetitions = 10,
                        Sets = 3
                    },
                    new Exercise
                    {
                        Name = "Rotator cuff strenghtening III",
                        Guide = new Guide
                        {
                            Description = "Exercise guide"
                        },
                        Weight = 4,
                        Repetitions = 10,
                        Sets = 5
                    }
                };

            AvailableWeeklyQuestionnaire = false;
            SessionsFinished = false;
            WeekNumber = 0;

            return Exercises.ToString();
        }
    }
}