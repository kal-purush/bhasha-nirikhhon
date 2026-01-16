using Macquarie.Handbook.Data.Course;
using Macquarie.Handbook.Data.Shared;
using MQHandbookAPI.Models.Macquarie.Handbook.Data.Shared;

namespace MQHandbookAPI.Models.Macquarie.Handbook.Data.Course;

public record CourseDataDto : MetadataDto
{
    public string AqfLevel { get; init; }  = string.Empty;
    public List<string>? AccreditingBodies { get; init; }
    public string AbbreviatedNameAndMajor { get; init; } = string.Empty;
    public string CourseCode { get; init; } = string.Empty;
    public string AbbreviatedName { get; init; } = string.Empty;
    //public KeyValueIdType Source { get; init; }
    public bool Active { get; init; }
    public DateTime? EffectiveDate { get; init; }
    public string LearningAndTeachingMethods { get; init; } = string.Empty;
    public string OverviewAndAimsOfTheCourse { get; init; } = string.Empty;
    public string SupportForLearning { get; init; } = string.Empty;
    public string GraduateDestinationsAndEmployability { get; init; } = string.Empty;
    public string FitnessToPractice { get; init; } = string.Empty;
    public string IndependentResearch { get; init; } = string.Empty;
    public string JustifyCapstoneUnit { get; init; } = string.Empty;
    public string HowWillStudentsMeetClosInThisDuration { get; init; } = string.Empty;
    public string WhatIsTheInternalStructureOfCourseMajors { get; init; } = string.Empty;
    public string OtherDoubleDegreeConsiderations { get; init; } = string.Empty;
    public string CourseStandardsAndQuality { get; init; } = string.Empty;
    public string Exit { get; init; } = string.Empty;
    public bool PartTime { get; init; }
    public string Structure { get; init; } = string.Empty;
    public bool NoEnrolment { get; init; }
    public string PublicationInformation { get; init; } = string.Empty;
    public string InternshipPlacement { get; init; } = string.Empty;
    public string Specialisations { get; init; } = string.Empty;
    public string GovtSpecialCourseType { get; init; } = string.Empty;
    public List<string>? EntryList { get; init; }
    public bool EntryGuarantee { get; init; }
    public bool PoliceCheck { get; init; }
    public string Year12Prerequisites { get; init; } = string.Empty;
    public string LastReviewDate { get; init; } = string.Empty;
    public string CareerOpportunities { get; init; } = string.Empty;
    public string Location { get; init; } = string.Empty;
    public string CourseValue { get; init; } = string.Empty;
    public string AlternativeExits { get; init; } = string.Empty;
    public string Progression { get; init; } = string.Empty;
    public string EnglishLanguage { get; init; } = string.Empty;
    public string IbMaths { get; init; } = string.Empty;
    public string Requirements { get; init; } = string.Empty;
    public string QualificationRequirement { get; init; } = string.Empty;
    public string ArticulationArrangement { get; init; } = string.Empty;
    public string PartnerFaculty { get; init; } = string.Empty;
    public bool FullTime { get; init; }
    public string Qualifications { get; init; } = string.Empty;
    public string DoubleDegrees { get; init; } = string.Empty;
    public bool InternationalStudents { get; init; }
    public string SpecialAdmission { get; init; } = string.Empty;
    public string Entry { get; init; } = string.Empty;
    public string Atar { get; init; } = string.Empty;
    public DateTime? CourseDataUpdated { get; init; }
    public string PriorLearningRecognition { get; init; } = string.Empty;
    public string VceOther { get; init; } = string.Empty;
    public string AwardsTitles { get; init; } = string.Empty;
    public bool OnCampus { get; init; }
    public string ResearchAreas { get; init; } = string.Empty;
    public string AccrediationStartDate { get; init; } = string.Empty;
    public string ParticiptionEnrolment { get; init; } = string.Empty;
    public string VceEnglish { get; init; } = string.Empty;
    public string IbOther { get; init; } = string.Empty;
    public bool Online { get; init; }
    public string ProgressToMasters { get; init; } = string.Empty;
    public string AdditionalInfo { get; init; } = string.Empty;
    public bool CriscosDisclaimerApplicable { get; init; }
    public string OtherDescription { get; init; } = string.Empty;
    public bool Other { get; init; }
    public bool HealthRecordsAndPrivacy { get; init; }
    public bool InformationDeclaration { get; init; }
    public string Ahegs { get; init; } = string.Empty;
    public bool ProhibitedEmploymentDeclaration { get; init; }
    public string MinimumEntryRequirements { get; init; } = string.Empty;
    public string EccrediationEndDate { get; init; } = string.Empty;
    public string AccrediationEnd { get; init; } = string.Empty;
    public string PostNominals { get; init; } = string.Empty;
    public string IbEnglish { get; init; } = string.Empty;
    public string CreditArrangements { get; init; } = string.Empty;
    public string Outcomes { get; init; } = string.Empty;
    public string MajorMinors { get; init; } = string.Empty;
    public string VceMaths { get; init; } = string.Empty;
    public string DegreesAwarded { get; init; } = string.Empty;
    public string NonYear12Entry { get; init; } = string.Empty;
    public bool WorkingWithChildrenCheck { get; init; }
    public string EntryPathwaysAndAdjustmentFactorsOtherDetails { get; init; } = string.Empty;
    public List<string>? EntryPathwaysAndAdjustmentFactors { get; init; }
    public bool DoesUndergraduatePrinciple_26_3Apply { get; init; }
    public bool FormalArticulationPathwayToHigherAward { get; init; }
    public string ApplicationMethodOtherDetails { get; init; } = string.Empty;
    public string IeltsOverallScore { get; init; } = string.Empty;
    public string IsThisAnAcceleratedCourse { get; init; } = string.Empty;
    public string HowDoesThisCourseDeliverACapstoneExperience { get; init; } = string.Empty;
    public string HoursPerWeek { get; init; } = string.Empty;
    public string ExclusivelyAnExitAward { get; init; } = string.Empty;
    public string IeltsListeningScore { get; init; } = string.Empty;
    public string AdmissionToCombinedDouble { get; init; } = string.Empty;
    public string IELTS_speaking_score { get; init; } = string.Empty;
    public string CapstoneOrProfessionalPractice { get; init; } = string.Empty;
    public string ExternalBody { get; init; } = string.Empty;
    public string OtherProviderName { get; init; } = string.Empty;
    public string ProviderNameAndSupportingDocumentation { get; init; } = string.Empty;
    public string Arrangements { get; init; } = string.Empty;
    public string NumberOfWeeks { get; init; } = string.Empty;
    public List<string>? ApplicationMethod { get; init; }
    public bool DeliveryWithThirdPartyProvider { get; init; }
    public bool AreThereAdditionalAdmissionPoints { get; init; }
    public string VolumeOfLearning { get; init; } = string.Empty;
    public string AwardAbbreviation { get; init; } = string.Empty;
    public string AdmissionRequirements { get; init; } = string.Empty;
    public bool AnyDoubleDegreeExclusions { get; init; }
    public string IeltsWritingScore { get; init; } = string.Empty;
    public string AhegsAwardText { get; init; } = string.Empty;
    public bool WorkBasedTrainingComponent { get; init; }
    public string IeltsReadingScore { get; init; } = string.Empty;
    public string WamRequiredForProgression { get; init; } = string.Empty;
    public string AccrediationTextForAhegs { get; init; } = string.Empty;
    public string ProviderName { get; init; } = string.Empty;
    public string AssessmentRegulations { get; init; } = string.Empty;
    public bool AccrediatedByExternalBody { get; init; }
    public bool OfferedByAnExternalProvider { get; init; }
    public string Assessment { get; init; } = string.Empty;
    public List<OrgUnitDataDto>? Level2OrgUnitData { get; init; }
    public List<string>? RelatedAssociatedItems { get; init; }
    public List<OfferingDto>? Offering { get; init; }
    public List<string>? StudyModes { get; init; }
    public List<AdmissionRequirementPointDto>? AdditionalAdmissionPoints { get; init; }
    public List<string>? CourseRules { get; init; }
    public List<CourseNoteDto>? CourseNotes { get; init; }
    public List<LearningOutcomeDto>? LearningOutcomes { get; init; }
    public List<HigherLevelCoursesThatStudentsMayExitFrom>? HigherLevelCoursesThatStudentsMayExitFrom { get; init; }
    public List<OrgUnitDataDto>? Level1OrgUnitData { get; init; }
    public List<Articulation>? Articulations { get; init; }
    public string CourseSearchTitle { get; init; } = string.Empty;
    public string AvailableInDoubles { get; init; } = string.Empty;
    public string AvailableDoubles { get; init; } = string.Empty;
    public string AvailableAOS { get; init; } = string.Empty;

    public string ExtId { get; init; } = string.Empty;

    public string VersionName { get; init; } = string.Empty;

    public List<FeeDto>? Fees { get; init; }

    public string CourseDurationInYears { get; init; } = string.Empty;

    public string MaximumDuration { get; init; } = string.Empty;

    public string FullTimeDuration { get; init; } = string.Empty;

    public string PartTimeDuration { get; init; } = string.Empty;

    public string FeesDescription { get; init; } = string.Empty;

    public string CricosCode { get; init; } = string.Empty;
}