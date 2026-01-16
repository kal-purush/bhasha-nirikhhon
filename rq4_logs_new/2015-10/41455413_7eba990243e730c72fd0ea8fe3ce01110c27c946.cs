using Microsoft.VisualStudio.TestTools.UnitTesting;
using projeto_integrado_2_sem.Validators;
using projeto_integrado_2_sem.Models;
using System;

namespace UnitTestProject1
{
    [TestClass]
    public class PasswordValidatorUnitTest
    {
        private User user;

        [TestInitialize]
        public void Initialize_User()
        {
            user = new User();
        }

        [TestCleanup]
        public void Clean_User()
        {
            user = null;
        }

        // Error Checks
        [TestMethod]
        public void Password_LessThanSixKeysTest()
        {
            ShouldIncludeError("123ab", PasswordValidator.Error.TOO_SHORT);
            ShouldNotIncludeError("abcd56", PasswordValidator.Error.TOO_SHORT);
        }

        [TestMethod]
        public void Password_MoreThanTenKeysTest()
        {
            ShouldIncludeError("123abc-*á01", PasswordValidator.Error.TOO_LONG);
            ShouldNotIncludeError("123abc7890", PasswordValidator.Error.TOO_LONG);
        }

        [TestMethod]
        public void Password_DistinctCases()
        {
            ShouldIncludeError("123abc7890", PasswordValidator.Error.NO_DISTINC_CASES);
            ShouldNotIncludeError("123aBc7890", PasswordValidator.Error.NO_DISTINC_CASES);
        }

        [TestMethod]
        public void Password_NoBlankSpaces()
        {
            ShouldIncludeError("123ab 7890", PasswordValidator.Error.SPACES);
            ShouldIncludeError("123ab\t7890", PasswordValidator.Error.SPACES);
            ShouldIncludeError("123ab\n7890", PasswordValidator.Error.SPACES);
            ShouldIncludeError("123ab\r7890", PasswordValidator.Error.SPACES);
            ShouldNotIncludeError("123aBc7890", PasswordValidator.Error.SPACES);
        }

        [TestMethod]
        public void Password_NoSpecialChars()
        {
            ShouldIncludeError("123ab☺00", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldIncludeError("123ab^000", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldIncludeError("1_3ab000", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldIncludeError("123ab*7890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldIncludeError("123áb7890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldIncludeError("123b\x7890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldNotIncludeError("123aBc7890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldNotIncludeError("123aBc\n890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldNotIncludeError("123aBc\r890", PasswordValidator.Error.SPECIAL_CHARS);
            ShouldNotIncludeError("123aBc\t890", PasswordValidator.Error.SPECIAL_CHARS);
        }

        [TestMethod]
        public void Password_LessThanTwoNumbers()
        {
            ShouldIncludeError("abcde1", PasswordValidator.Error.TOO_FEW_NUMBERS);
            ShouldNotIncludeError("abcde12", PasswordValidator.Error.TOO_FEW_NUMBERS);
        }

        [TestMethod]
        public void Password_WhenLessThanTwoLetters()
        {
            ShouldIncludeError("a123456", PasswordValidator.Error.TOO_FEW_LETTERS);
            ShouldNotIncludeError("abCde12", PasswordValidator.Error.TOO_FEW_LETTERS);
        }

        [TestMethod]
        public void Password_RepeatedLetters()
        {
            ShouldIncludeError("aaa1234", PasswordValidator.Error.REPEATED_LETTERS);
            ShouldIncludeError("1234bbbv", PasswordValidator.Error.REPEATED_LETTERS);
            ShouldIncludeError("123bBb1v", PasswordValidator.Error.REPEATED_LETTERS);
            ShouldIncludeError("1aaaa34", PasswordValidator.Error.REPEATED_LETTERS);
            ShouldIncludeError("a1aaa34", PasswordValidator.Error.REPEATED_LETTERS);
            ShouldNotIncludeError("aabcd12", PasswordValidator.Error.REPEATED_LETTERS);
        }

        [TestMethod]
        public void Password_RepeatedNumbers()
        {
            ShouldIncludeError("111mcnd", PasswordValidator.Error.REPEATED_NUMBERS);
            ShouldIncludeError("1111mcdn", PasswordValidator.Error.REPEATED_NUMBERS);
            ShouldIncludeError("aV888fx", PasswordValidator.Error.REPEATED_NUMBERS);
            ShouldIncludeError("vf5555Ds", PasswordValidator.Error.REPEATED_NUMBERS);
            ShouldIncludeError("4ses777", PasswordValidator.Error.REPEATED_NUMBERS);
            ShouldNotIncludeError("44advdf", PasswordValidator.Error.REPEATED_NUMBERS);
        }

        [TestMethod]
        public void Password_EqualPreviousPassword()
        {
            testUser().oldPassword = "aV898fxI";

            ShouldIncludeError("aV898fxI", PasswordValidator.Error.EQUALS_PREVIOUS_PASSWORD);
            ShouldNotIncludeError("44advdf", PasswordValidator.Error.EQUALS_PREVIOUS_PASSWORD);
        }

        [TestMethod]
        public void Password_EqualCode()
        {
            testUser().code = "126534";

            ShouldIncludeError("126534", PasswordValidator.Error.EQUALS_USER_CODE);
            ShouldNotIncludeError("44advdf", PasswordValidator.Error.EQUALS_USER_CODE);
        }

        // Quality Checks

        [TestMethod]
        public void Password_SequencialNumbers()
        {
            ShouldIncludeWarning("1234", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("12340", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("056789", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("2468", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("4321", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("97531", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);

            ShouldIncludeWarning("acv1234", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("av1234es", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldIncludeWarning("rv46789es", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);

            ShouldNotIncludeWarning("456advdf", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
            ShouldNotIncludeWarning("456ad7vdf", PasswordValidator.Warning.SEQUENCIAL_NUMBERS);
        }

        [TestMethod]
        public void Password_OnlyTwoLetters()
        {
            ShouldIncludeWarning("a1b1234", PasswordValidator.Warning.ONLY_TWO_LETTERS);
            ShouldNotIncludeWarning("s2d0g330", PasswordValidator.Warning.ONLY_TWO_LETTERS);
        }

        [TestMethod]
        public void Password_OnlyTwoNumbers()
        {
            ShouldIncludeWarning("fs1rsd2", PasswordValidator.Warning.ONLY_TWO_NUMBERS);
            ShouldNotIncludeWarning("s2dg3df0", PasswordValidator.Warning.ONLY_TWO_NUMBERS);
        }

        [TestMethod]
        public void Password_ContainsUserCode()
        {
            testUser().code = "1Acd451";

            ShouldIncludeWarning("rg1Acd451dd", PasswordValidator.Warning.CONTAINS_USER_CODE);
            ShouldIncludeWarning("rg1acd451dd", PasswordValidator.Warning.CONTAINS_USER_CODE);

            ShouldNotIncludeWarning("rg1cad451dd", PasswordValidator.Warning.CONTAINS_USER_CODE);
            ShouldNotIncludeWarning("rg1cAd451dd", PasswordValidator.Warning.CONTAINS_USER_CODE);
        }

        [TestMethod]
        public void Password_ContainsUserFirstName()
        {
            testUser().name = "Lucas Cone Top";

            ShouldIncludeWarning("OlucasEcone", PasswordValidator.Warning.CONTAINS_NAME);
            ShouldIncludeWarning("oLucasEcone", PasswordValidator.Warning.CONTAINS_NAME);
            ShouldIncludeWarning("OluCasEcone", PasswordValidator.Warning.CONTAINS_NAME);

            ShouldNotIncludeWarning("olunaoecone", PasswordValidator.Warning.CONTAINS_NAME);
        }

        [TestMethod]
        public void Password_ContainsUserInitials()
        {
            testUser().name = "Lucas Muito Cone";

            ShouldIncludeWarning("pqqLmClalala", PasswordValidator.Warning.CONTAINS_NAME_INITIALS);
            ShouldIncludeWarning("LMClalala", PasswordValidator.Warning.CONTAINS_NAME_INITIALS);
            ShouldIncludeWarning("lalalalmc", PasswordValidator.Warning.CONTAINS_NAME_INITIALS);

            ShouldNotIncludeWarning("cml", PasswordValidator.Warning.CONTAINS_NAME_INITIALS);
        }

        [TestMethod]
        public void Password_ContainsUserFullBirthDate()
        {
            testUser().birthDate = new DateTime(1995, 01, 25);

            ShouldIncludeWarning("l2k25011995dd", PasswordValidator.Warning.CONTAINS_FULL_BIRTH_DATE);
            ShouldIncludeWarning("025011995dd", PasswordValidator.Warning.CONTAINS_FULL_BIRTH_DATE);
            ShouldIncludeWarning("ert25011995", PasswordValidator.Warning.CONTAINS_FULL_BIRTH_DATE);

            ShouldNotIncludeWarning("ert25011995", PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldNotIncludeWarning("ert02251995dd", PasswordValidator.Warning.CONTAINS_FULL_BIRTH_DATE);
        }

        [TestMethod]
        public void Password_ContainsUserBirthDateDayMonth()
        {
            testUser().birthDate = new DateTime(1995, 01, 25);

            ShouldIncludeWarning("l22501dd", PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning("02501df", PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning("ert25010", PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);

            ShouldNotIncludeWarning("ert02251995dd", PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
        }

        [TestMethod]
        public void Password_ContainsReverseUserFullBirthDate()
        {
            testUser().birthDate = new DateTime(1995, 01, 25);

            ShouldIncludeWarning("l2k19950125dd", PasswordValidator.Warning.CONTAINS_REVERSE_FULL_BIRTH_DATE);
            ShouldIncludeWarning("019950125dd", PasswordValidator.Warning.CONTAINS_REVERSE_FULL_BIRTH_DATE);
            ShouldIncludeWarning("ert19950125", PasswordValidator.Warning.CONTAINS_REVERSE_FULL_BIRTH_DATE);

            ShouldNotIncludeWarning("ert19950125", PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldNotIncludeWarning("ert02251995dd", PasswordValidator.Warning.CONTAINS_REVERSE_FULL_BIRTH_DATE);
        }

        [TestMethod]
        public void Password_ContainsReverseUserBirthDateDayMonth()
        {
            testUser().birthDate = new DateTime(1995, 01, 25);

            ShouldIncludeWarning("l20125dd", PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning("00125df", PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning("ert0125", PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);

            ShouldNotIncludeWarning("ert02251995dd", PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
        }

        // When Too weak
        [TestMethod]
        public void Password_WhenTooWeakZeroScore()
        {
            testUser().name = "Lucas Cone";
            testUser().birthDate = new DateTime(1994, 1, 23);
            var password = "Lc01232301";

            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_NAME_INITIALS);
            ShouldIncludeWarning(password, PasswordValidator.Warning.ONLY_TWO_LETTERS);
            ShouldIncludeWarning(password, PasswordValidator.Warning.SEQUENCIAL_NUMBERS);

            var validationResult = validationResultFor(testUser(), password);
            Assert.AreEqual(validationResult.errors.Count, 1);
            Assert.AreEqual(validationResult.score(), 0);

            ShouldIncludeError(password, PasswordValidator.Error.TOO_WEAK);
        }

        [TestMethod]
        public void Password_WhenTooWeakTwoScore()
        {
            testUser().name = "Lucas Cone";
            testUser().birthDate = new DateTime(1994, 1, 23);
            var password = "Ld01232301";

            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.ONLY_TWO_LETTERS);
            ShouldIncludeWarning(password, PasswordValidator.Warning.SEQUENCIAL_NUMBERS);

            var validationResult = validationResultFor(testUser(), password);
            Assert.AreEqual(validationResult.errors.Count, 1);
            Assert.AreEqual(validationResult.score(), 2);

            ShouldIncludeError(password, PasswordValidator.Error.TOO_WEAK);
        }

        // Valid Passwords
        [TestMethod]
        public void Password_WhenThreeScore()
        {
            testUser().name = "Lucas Cone";
            testUser().birthDate = new DateTime(1994, 12, 23);
            testUser().code = "54";
            var password = "Lc231254d";

            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_USER_CODE);
            ShouldIncludeWarning(password, PasswordValidator.Warning.CONTAINS_NAME_INITIALS);

            var validationResult = validationResultFor(testUser(), password);
            Assert.AreEqual(validationResult.valid(), true);
            Assert.AreEqual(validationResult.score(), 3);
        }

        [TestMethod]
        public void Password_WhenStrong()
        {
            testUser().name = "Lucas Cone";
            testUser().birthDate = new DateTime(1994, 12, 23);
            testUser().code = "54";

            var password = "As495c02d";
            var validationResult = validationResultFor(testUser(), password);

            Assert.AreEqual(validationResult.valid(), true);
            Assert.AreEqual(validationResult.score(), 10);
        }

        // Helper Methods
        private void ShouldIncludeError(string password, PasswordValidator.Error error)
        {
            var errors = validationResultFor(testUser(), password).errors;
            CollectionAssert.Contains(errors, error);
        }

        private void ShouldNotIncludeError(string password, PasswordValidator.Error error)
        {
            var errors = validationResultFor(testUser(), password).errors;
            CollectionAssert.DoesNotContain(errors, error);
        }

        private void ShouldIncludeWarning(string password, PasswordValidator.Warning warn)
        {
            var warns = validationResultFor(testUser(), password).warnings;
            CollectionAssert.Contains(warns, warn);
        }

        private void ShouldNotIncludeWarning(string password, PasswordValidator.Warning warn)
        {
            var warns = validationResultFor(testUser(), password).warnings;
            CollectionAssert.DoesNotContain(warns, warn);
        }

        private PasswordValidator.ValidaionResult validationResultFor(User user, string password)
        {
            return (new PasswordValidator()).ValidatePassword(user, password);
        }

        private User testUser()
        {
            return this.user;
        }
    }
}