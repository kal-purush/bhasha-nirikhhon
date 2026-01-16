namespace Macquarie.Handbook.Data.Unit.Prerequisites.Parser;

using System;
using System.Text.RegularExpressions;
using Macquarie.Handbook.Data.Transcript;
using Macquarie.Handbook.Data.Transcript.Facts;
using Macquarie.Handbook.Data.Unit.Prerequisites.Facts;

public class UnitFactParser : IPrerequisiteParser
{
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(50);
    // Regex to capture all unit code variations
    private static readonly Regex regexUnitCode = new(@"\b([A-Z]{3,4})(\s?)([\d]{3,4})", RegexOptions.Compiled, RegexTimeout);
    //Matches (P|CR|D|HD) elements
    private static readonly Regex _regexGradeRequirement = new(@"\((P|CR|D|HD)\)", RegexOptions.Compiled, RegexTimeout);

    public IRequirementFact Parse(string sourceText) {
        //String navigator
        var split = sourceText.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        for (int i = 0; i < split.Length; i++) {
            string word = split[i];

            var unitcodeMatch = regexUnitCode.Match(word);

            if (unitcodeMatch.Success) {
                //See if the grade is in the unit code string
                var gradeRequirementMatch = _regexGradeRequirement.Match(word);

                // If the grade is not, check the next symbol.
                if (!gradeRequirementMatch.Success)
                    if (i + 1 < split.Length)
                        gradeRequirementMatch = _regexGradeRequirement.Match(split[i + 1]);

                EnumGrade grade = EnumGrade.Pass;
                // If we have the grade match, convert from string to grade.
                if (gradeRequirementMatch.Success)
                    grade = GradeConverter.ConvertStringToGrade(gradeRequirementMatch.Value[1..^1]);
                //Decide if final grade check is required

                return new UnitRequirementFact(new UnitFact()
                {
                    UnitCode = unitcodeMatch.Value,
                    Results = new(null, grade)
                });
            }

        }

        return null;
    }
}