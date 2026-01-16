using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace projeto_integrado_2_sem.Validators
{
    public class PasswordValidator
    {
        public enum Error 
        { 
            TOO_SHORT=0, 
            TOO_LONG, 
            SPACES,
            SPECIAL_CHARS,
            NO_DISTINC_CASES, 
            TOO_FEW_LETTERS, 
            TOO_FEW_NUMBERS,
            REPEATED_LETTERS, 
            REPEATED_NUMBERS, 
            EQUALS_PREVIOUS_PASSWORD, 
            EQUALS_USER_CODE 
        }

        public enum Warning
        {
            SEQUENCIAL_NUMBERS=0,
            ONLY_TWO_LETTERS,
            ONLY_TWO_NUMBERS,
            CONTAINS_USER_CODE,
            CONTAINS_NAME,
            CONTAINS_NAME_INITIALS,
            CONTAINS_FULL_BIRTH_DATE,
            CONTAINS_DAY_AND_MONTH_OF_BIRTH_DATE,
            CONTAINS_REVERSE_FULL_BIRTH_DATE,
            CONTAINS_REVERSE_DAY_AND_MONTH_OF_BIRTH_DATE,
        }

        public int[] WarningWeight = new int[] { 1, 1, 1, 2, 2, 2, 3, 3 };

        public class ValidaionResult
        {
            public List<Error> errors;
            public List<Warning> warnings;
        }
    }
}