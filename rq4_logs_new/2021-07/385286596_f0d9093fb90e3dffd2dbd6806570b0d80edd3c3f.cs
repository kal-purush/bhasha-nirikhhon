using AdmissionSystem2.Entites;
using AdmissionSystem2.Models;
using AutoMapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AdmissionSystem2.Services
{
    public class AdminRepo:IAdminRepo
    {
        private AdmissionSystemDbContext _AdmissionSystemDbContext;
        private IMapper _Mapper;

        public AdminRepo(AdmissionSystemDbContext admissionSystemDbContext, IMapper Mapper)
        {
            _AdmissionSystemDbContext = admissionSystemDbContext;
            _Mapper = Mapper;
        }
        public MedicalHistory GetMedicalHistory(int applicantId)
        {
            return _AdmissionSystemDbContext.MedicalHistory.FirstOrDefault(a => a.ApplicantId == applicantId);
        }
        public AdmissionDetails GetAdmissionDetails(int applicantId, Guid AdmissionDetailsId)
        {
            return _AdmissionSystemDbContext.AdmissionDetails.Where(a => a.ApplicantId == applicantId && a.Id == AdmissionDetailsId).FirstOrDefault();
        }
        public MedicalHistory GetMedicalHistory(int applicantId, Guid MedicalHistoryId)

        {
            return _AdmissionSystemDbContext.MedicalHistory.Where(a => a.ApplicantId == applicantId).FirstOrDefault();
        }
        public Sibling GetSibling(int applicantId, Guid siblingId)
        {
            return _AdmissionSystemDbContext.Sibling.Where(a => a.ApplicantId == applicantId && a.SibilingId == siblingId).FirstOrDefault();
        }
        public IEnumerable<Sibling> GetSiblings(int applicantId)
        {
            return _AdmissionSystemDbContext.Sibling.Where(a => a.ApplicantId == applicantId).OrderBy(a => a.SiblingName).ToList();
        }
        public EmergencyContact GetEmergencyContact(int ApplicantId, Guid Id)
        {
            return _AdmissionSystemDbContext.EmergencyContact.FirstOrDefault(a => a.ApplicantId == ApplicantId && a.Id == Id);
        }
        public Document GetDocument(int ApplicantId, int DocumentId)
        {
            return _AdmissionSystemDbContext.Documents.FirstOrDefault(a => a.ApplicantId == ApplicantId && a.Id == DocumentId);
        }
        public IEnumerable<ParentInfo> GetParentsInfos(int ApplicantId)
        {
            return _AdmissionSystemDbContext.ParentInfo.Where(a => a.ApplicantId == ApplicantId).ToList();

        }
        public ParentInfo GetParentInfos(int ApplicantId, string Gender)
        {
            return _AdmissionSystemDbContext.ParentInfo.Where(a => a.ApplicantId == ApplicantId && a.Gender == Gender).FirstOrDefault();

        }
        public AdmissionDetails GetAdmissionDetails(int ApplicantId)
        {
            return _AdmissionSystemDbContext.AdmissionDetails.Where(a => a.ApplicantId == ApplicantId).FirstOrDefault();
        }
        public IEnumerable<EmergencyContact> GetEmergencyContacts(int ApplicantId)
        {
            return _AdmissionSystemDbContext.EmergencyContact.Where(a => a.ApplicantId == ApplicantId).ToList();
        }
        public IEnumerable<Document> GetDocuments(int ApplicantId)
        {
            return _AdmissionSystemDbContext.Documents.Where(a => a.ApplicantId == ApplicantId).ToList();
        }
        public ParentInfo ParentInfoExist(int ApplicantId, Guid ParentInfoId)
        {
            return _AdmissionSystemDbContext.ParentInfo.FirstOrDefault(a => a.ApplicantId == ApplicantId && a.Id == ParentInfoId);
        }
        public Applicant GetApplicant(int _ApplicantId)
        {
            var Applicant = _AdmissionSystemDbContext.Applicant.FirstOrDefault(a => a.ApplicantId == _ApplicantId);
            return Applicant;

        }
        public void DeleteSibling(Sibling sibling)
        {
            _AdmissionSystemDbContext.Sibling.Remove(sibling);
        }
        public Application GetApplication(int ApplicantId)
        {
            Application Application = new Application();
            Application.Applicant = _AdmissionSystemDbContext.Applicant.FirstOrDefault(a => a.ApplicantId == ApplicantId);
            Application.AdmissionDetails = _AdmissionSystemDbContext.AdmissionDetails.FirstOrDefault(a => a.ApplicantId == ApplicantId);
            Application.EmergencyContact = GetEmergencyContacts(ApplicantId);
            Application.Sibling = GetSiblings(ApplicantId);
            Application.MedicalHistory = _Mapper.Map<MedicalHistoryDto>(GetMedicalHistory(ApplicantId));

            Application.ParentInfo = GetParentsInfos(ApplicantId);
            ///   Application.Documents = GetDocuments(ApplicantId);
            return Application;

        }
        public bool Save()
        {
            return (_AdmissionSystemDbContext.SaveChanges() >= 0);
        }
    }
}