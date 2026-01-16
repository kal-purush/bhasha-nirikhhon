using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.IO;
using System.Xml.Serialization;

namespace hogward.Windows.Student
{
    /// <summary>
    /// Interaction logic for StudentLessonSelection.xaml
    /// </summary>
    public partial class StudentLessonSelection : Window
    {
        
        public StudentLessonSelection()
        {
            InitializeComponent();

            var studetns = Program.StudentDetecter();
            string[] userid = File.ReadAllText("UserIndex.txt").Split(" ");
            string[] resualt =  Program.UserFounder(userid[0], userid[1]);
            StudentLessonsSelection.Title = resualt[0] + " " + resualt[1];
            int count = Convert.ToInt32(resualt[2]);
            AllowClass(studetns[count].Term);
        }

        public void AllowClass(int term)
        {
            var lessons = Program.LessonDetecter();
            for(int i = 0; i< lessons.Length; i++)
            {
                if (lessons[i].PresentationSemester == term)
                {
                    if (lessons[i].Name == "Chemistry")
                    {
                        Chemistry.IsEnabled = true;
                        Chemistry.ItemsSource = Program.AllowProfesorLesson("Chemistry");
                    }
                    else if(lessons[i].Name == "BotanicalOne")
                    {
                        BotanicalOne.IsEnabled = true;
                        BotanicalOne.ItemsSource = Program.AllowProfesorLesson("BotanicalOne");
                    }
                    else if(lessons[i].Name == "BotanicalTwo")
                    {
                        BotanicalTwo.IsEnabled = true;
                        BotanicalTwo.ItemsSource = Program.AllowProfesorLesson("BotanicalTwo");
                    }
                    else if(lessons[i].Name == "BotanicalThree")
                    {
                        BotanicalThree.IsEnabled = true;
                        BotanicalThree.ItemsSource = Program.AllowProfesorLesson("BotanicalThree");
                    }
                    else if (lessons[i].Name == "BotanicalFour")
                    {
                        BotanicalFour.IsEnabled = true;
                        BotanicalFour.ItemsSource = Program.AllowProfesorLesson("BotanicalFour");
                    }
                    else if (lessons[i].Name == "Occultism")
                    {
                        Occultism.IsEnabled = true;
                        Occultism.ItemsSource = Program.AllowProfesorLesson("Occultism");
                    }
                    else if (lessons[i].Name == "Sport")
                    {
                        Sport.IsEnabled = true;
                        Sport.ItemsSource = Program.AllowProfesorLesson("Sport");
                    }
                    else if (lessons[i].Name == "Plant")
                    {
                        Plant.IsEnabled = true;
                        Plant.ItemsSource = Program.AllowProfesorLesson("Plant");
                    }
                }
            }
        }

        private void BotanicalOne_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("BotanicalOne");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void BotanicalTwo_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("BotanicalTwo");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void BotanicalThree_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("BotanicalThree");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void BotanicalFour_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("BotanicalFour");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void Occultism_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("Occultism");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void Sport_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("Sport");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }

        private void Plant_Click(object sender, RoutedEventArgs e)
        {
            using (var Writer = new StreamWriter("LessonsDeteail.txt"))
            {
                Writer.Write("Plant");
            }
            Lessons lesson = new Lessons();
            lesson.Show();
        }
    }
}