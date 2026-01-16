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
    public class FractionFigEqualityExam
    {
        public async Task<EqualityExamDto<FractionFigEqualityDto>> PrepareExam(int difficultyLevel,
                                                                        EKlubasUser user,
                                                                        ApplicationDbContext _context,
                                                                        int passMark,
                                                                        int reward,
                                                                        int durationInMinutes)
        {
            var equalityExam = new EqualityExamDto<FractionFigEqualityDto>();
            int numerator, denominator = 0;
            var studyExam = new StudyExam(passMark, reward, durationInMinutes, user);

            for (int i = 0; i < 30; i++)
            {
                numerator = MathServices.GetRandomNumber(1, 5);
                denominator = numerator + MathServices.GetRandomNumber(0, 5);
                var fraction = new Fraction(numerator, denominator);
                var drawingFraction = new Fraction(fraction.Numerator, fraction.Denominator);

                RandomizeDrawingFraction(drawingFraction);
                var userAnswer = GetExamAnswer(fraction, drawingFraction);

                var fractionTask = new FractionFigEqualityDto(fraction, drawingFraction);

                equalityExam.Tasks.Add(userAnswer.Id, fractionTask);
                studyExam.StudyExamResults.Add(userAnswer);
            }

            equalityExam.StudyExam = studyExam;
            await _context.StudyExams.AddAsync(studyExam);
            await _context.SaveChangesAsync();

            return equalityExam;
        }

        private static StudyExamAnswer GetExamAnswer(Fraction fraction, Fraction drawingFraction)
        {
            var answerId = Guid.NewGuid();
            var answer = fraction.GetFractionDecimalForm() == drawingFraction.GetFractionDecimalForm() ? "Teisinga" : "Neteisinga";

            var userAnswer = new StudyExamAnswer(answerId, answer);
            return userAnswer;
        }

        private static void RandomizeDrawingFraction(Fraction drawingFraction)
        {
            bool randomizeDenominator = MathServices.GetRandomNumber(0, 100) % 2 == 0 ? true : false;
            if (randomizeDenominator)
            {
                drawingFraction.Numerator = MathServices.GetRandomNumber(1, 7);
                drawingFraction.Denominator = drawingFraction.Numerator + MathServices.GetRandomNumber(0, 5);
            }
        }
    }
}