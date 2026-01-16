using System;

namespace IteaDelegates
{
    public delegate void OnLessonStart();

    public delegate void LessonEvent(object sender, LessonEventArgs e);

    public class Lesson
    {
        public string Str { get; set; }
        public OnLessonStart OnStart { get; set; } = () => Console.WriteLine("Anonym");

        public event LessonEvent OnLessonEvent;

        public Lesson()
        {
            OnStart += MyMethod;
        }

        public void MyMethod()
        {
            Console.WriteLine("DefaultOnStart method");
            OnLessonEvent?.Invoke(this, null);
        }
    }

    public class LessonEventArgs : EventArgs
    {
        public string LessonName { get; set; }
        public DateTime CurrentTime { get; set; }
    }
}