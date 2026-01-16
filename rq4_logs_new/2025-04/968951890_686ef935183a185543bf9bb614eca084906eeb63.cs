using Lucky7_Inventory_System_Application.Constants;
using Lucky7_Inventory_System_Domain.Entities;
using static Lucky7_Inventory_System_Application.Responses.ServiceResponses;

namespace Lucky7_Inventory_System_Application.Services
{
    public class ValidationService
    {
        // A method to check if a value is required and valid.
        public static GetResponse ValidateRequired<T>(T value, string fieldName)
        {
            if (EqualityComparer<T>.Default.Equals(value, default(T)) || (value is int intValue && intValue == 0))
            {
                return new GetResponse(false, null, $"{fieldName} is Required", StatusResponse.invalidoperation);
            }
            return null;
        }

        // A method to validate if an entity has required fields populated.
        public static List<GetResponse> ValidateUser(int roleId, int statusId)
        {
            var validationResults = new List<GetResponse>();

            var roleValidation = ValidateRequired(roleId, "Role");
            if (roleValidation != null)
                validationResults.Add(roleValidation);

            var statusValidation = ValidateRequired(statusId, "Status");
            if (statusValidation != null)
                validationResults.Add(statusValidation);

            return validationResults;
        }
    }
}