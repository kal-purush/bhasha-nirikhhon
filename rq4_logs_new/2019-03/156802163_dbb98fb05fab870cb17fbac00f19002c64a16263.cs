using BrainyStories.Objects;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;

namespace BrainyStories
{
    public class QuizFactory
    {
        public ObservableCollection<Quiz> GenerateQuizzes(String storyName)
        {
            ObservableCollection<Quiz> QuizTemp = new ObservableCollection<Quiz>();

            if (storyName.Equals("The Lion and the Mouse"))
            {
                QuizTemp.Add(new Quiz
                {
                    QuizName = "The Lion and the Mouse Quiz 1",
                    Score = 0,
                    Questions = null,
                    PlayTime = new TimeSpan(0,0,5)
                });
            }
            else if (storyName.Equals("The Little Red Hen"))
            {
               
            }
            else if (storyName.Equals("The Boy Who Cried Wolf"))
            {
               
            }
            else if (storyName.Equals("The Elves and Shoemaker"))
            {
               
            }
            else if (storyName.Equals("The Three Little Pigs"))
            {
                
            }
            else if (storyName.Equals("The Three Billy Goats Gruff"))
            {
                
            }
            else if (storyName.Equals("The Tale of Peter Rabbit"))
            {
                
            }
            else if (storyName.Equals("The Gingerbread Man"))
            {
               
            }
            else if (storyName.Equals("Rumplestiltskin"))
            {
                
            }
            else if (storyName.Equals("Little Red Riding Hood"))
            {
                
            }

            return QuizTemp;
        }
    }
}