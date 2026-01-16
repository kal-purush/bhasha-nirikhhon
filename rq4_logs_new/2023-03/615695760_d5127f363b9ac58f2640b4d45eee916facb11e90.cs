

namespace Lesson3
{
    public class Cardio : Doctor
    {
        public Cardio(string name, int age, string spec, int workexp) : base(name, age, spec, workexp)
        {

        }
        public override void Cure(Disease disease)
        {
            if (disease.HearthDis)
            {
                disease.HearthDis = false;
            }
            else 
            {
                Console.WriteLine($"Пациент {Name},у вас нет проблем с сердцем");  
            }
        }
    }
}