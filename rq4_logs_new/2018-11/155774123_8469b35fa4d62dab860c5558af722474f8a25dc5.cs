using Homework.Interfaces;
using Homework.Models;
using Homework.Utils;

namespace Homework
{
    public class CompetitionManager
    {
        ICompetitionReader _reader { get; }
        ICompetitionResultWriter _writer { get; }

        public CompetitionManager(ICompetitionReader reader, ICompetitionResultWriter writer)
        {
            _reader = reader;
            _writer = writer;
        }

        public void Execute()
        {
            var competitions = _reader.Read();

            var combination = DetermineWinnerCombination(competitions);

            _writer.Write(new CompetitionResult(combination.Applications));
        }

        private Combination DetermineWinnerCombination(Array<Application> applications)
        {
            var combinations = GetIndependentApplicationCombinations(applications);

            return combinations.Max((Combination combination, Combination max) =>
            {
                int sumProfit(Application application, int carry)
                {
                    return carry + application.Price;
                }

                return combination.Applications.Reduce(sumProfit, 0) > max.Applications.Reduce(sumProfit, 0);
            });
        }

        private Array<Combination> GetIndependentApplicationCombinations(Array<Application> applications)
        {
            var combinations = applications
                .Map((application) => new Combination(new Array<Application>(new[] { application })));

            bool newCreated;

            do
            {
                newCreated = false;

                foreach (var combination in combinations)
                {
                    foreach (var application in applications)
                    {
                        if (combination.Contains(application) || combination.Intersects(application))
                        {
                            continue;
                        }

                        var copy = combination.Copy();
                        copy.Applications.Add(application);

                        if (combinations.Has((comb) => comb.SameAs(copy)))
                        {
                            continue;
                        }

                        combinations.Add(copy);

                        newCreated = true;
                    }
                }
            } while (newCreated);

            return combinations;
        }
    }
}