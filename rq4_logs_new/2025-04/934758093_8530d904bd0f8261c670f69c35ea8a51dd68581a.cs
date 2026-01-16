using LanguageSchoolApp.model;
using LanguageSchoolApp.model.Users;

namespace LanguageSchoolApp.service.Users.PenaltyPoints
{
    public interface IPenaltyPointService
    {
        Dictionary<int, PenaltyPoint> GetAllPenaltyPoints();
        List<PenaltyPoint> GetAllPenaltyPointsByIds(List<int> penaltyPointsIds);
        PenaltyPoint GetPenaltyPoint(int id);
        bool PenaltyPointExists(int id);
        void CreatePenaltyPoint(string studentId, string reasonStr);
        void DeletePenaltyPoint(int id);
        int GenerateId(string studentId, DateTime receivedDate, PenaltyReason reason);
    }
}