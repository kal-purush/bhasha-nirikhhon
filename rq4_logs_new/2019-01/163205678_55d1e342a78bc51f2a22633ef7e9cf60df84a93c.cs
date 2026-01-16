using EKlubas.Application;
using EKlubas.Common.Services;
using EKlubas.Domain;
using EKlubas.Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.UI.Services.MathExam
{
    public class FractionEqualityExam
    {
        public async Task<EqualityExamDto> PrepareFractionEqualityExam(int difficultyLevel,
                                                                        EKlubasUser user,
                                                                        ApplicationDbContext _context,
                                                                        int passMark,
                                                                        int reward,
                                                                        int durationInMinutes)
        {
            var equalityExam = new EqualityExamDto();
            var fractionLeft = new Fraction();
            var fractionRight = new Fraction();
            int numerator, denumerator = 0;
            var taskRandomizer = 0;
            var studyExam = new StudyExam(passMark, reward, durationInMinutes, user);

            for (int i = 0; i < 30; i++)
            {
                taskRandomizer = MathServices.GetRandomNumber();
                numerator = MathServices.GetRandomNumber(1, 5);
                denumerator = MathServices.GetRandomNumber(0, 5) + numerator;

                fractionLeft.SetFractionInformation(numerator, denumerator);

                if (taskRandomizer % 3 == 0)
                {
                    var fractionMultiplier = MathServices.GetRandomNumber(2, 5);
                    numerator = numerator * fractionMultiplier;
                    denumerator = denumerator * fractionMultiplier;

                    fractionRight.SetFractionInformation(numerator, denumerator);
                }
                else
                {
                    numerator = MathServices.GetRandomNumber(1, 5);
                    denumerator = MathServices.GetRandomNumber(0, 5) + numerator;

                    fractionRight.SetFractionInformation(numerator, denumerator);
                }

                var answerId = Guid.NewGuid();
                var answer = fractionLeft.GetFractionDecimalForm() == fractionRight.GetFractionDecimalForm() ? "Teisinga" : "Neteisinga";

                var userAnswer = new StudyExamAnswer(answerId, answer);
                var fractionTask = $"{fractionLeft.GetFractionHtml()} = {fractionRight.GetFractionHtml()}";

                equalityExam.Tasks.Add(userAnswer.Id, fractionTask);
                studyExam.StudyExamResults.Add(userAnswer);
            }

            equalityExam.StudyExam = studyExam;
            await _context.StudyExams.AddAsync(studyExam);
            await _context.SaveChangesAsync();

            return equalityExam;
        }
    }
}