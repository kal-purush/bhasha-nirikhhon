namespace BackEndFucntions
{
    public static class ValidateActivitiesInput
    {
        public static bool ValidateNumbers(List<dynamic> transports, List<dynamic> foods, List<dynamic> utilities)
        {
            // call each individual amount validator
            bool transportsValid = ValidateTransportAmounts(transports);
            bool foodsValid = ValidateFoodAmounts(foods);
            bool utilitiesValid = ValidateUtilityAmounts(utilities);

            // check if any are false, return false, otherwise true
            return transportsValid && foodsValid && utilitiesValid;
        }
        private static bool ValidateTransportAmounts(List<dynamic> transports)
        {
            // go through each amount in transports and check if its a number variable
            foreach (var transport in transports)
            {
                string amount = transport.Miles;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
        private static bool ValidateFoodAmounts(List<dynamic> foods)
        {
            // go through each amount in foods and check if its a number variable
            foreach (var food in foods)
            {
                string amount = food.Amount;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
        private static bool ValidateUtilityAmounts(List<dynamic> utilities)
        {
            // go through each utilities in transports and check if its a number variable
            foreach (var utility in utilities)
            {
                string amount = utility.Hours;
                if (!double.TryParse(amount, out _))
                {
                    return false; // Invalid non-number value found, return false
                }
            }
            return true; // All amounts are valid numbers
        }
    }
}