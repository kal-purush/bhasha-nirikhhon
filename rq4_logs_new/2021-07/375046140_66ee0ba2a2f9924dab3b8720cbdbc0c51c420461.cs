using System.Runtime.Serialization;

namespace Api.Errors
{
    public enum ApiErrorMessage
    {
        [EnumMember(Value = "Zasób tylko dla administratora")]
        AdminContent,
        
        #region User Messages
        
        [EnumMember(Value = "Login jest zarezerwowany dla innego punktu")]
        UserExist,
        [EnumMember(Value = "Nie ma takiego użytkownika")]
        UserNotExist,
        [EnumMember(Value = "Administrator już istnieje lub jest to login utworzony dla istniejącego punktu")]
        AdminExist,
        [EnumMember(Value = "Administrator nie został dodany")]
        AdminCreate,
        [EnumMember(Value = "Hasło nie zostało zmienione")]
        ChangePassword,
        [EnumMember(Value = "Nie uzupełniono wszystkich wymaganych pól")]
        MissingFields,
        
        #endregion
        
        #region Point Messages
        
        [EnumMember(Value = "Punkt o tej nazwie już istnieje")]
        PointExist,
        [EnumMember(Value = "Punkt nie został dodany")]
        PointCreate,
        
        #endregion
        
        #region Participant Messages
        
        [EnumMember(Value = "Pracownik został pomyślnie zaktualizowany")]
        ParticipantUpdatedSuccess,
        [EnumMember(Value = "Pracownik nie został zaktualizowany")]
        ParticipantUpdatedFailed

        #endregion
    }
}