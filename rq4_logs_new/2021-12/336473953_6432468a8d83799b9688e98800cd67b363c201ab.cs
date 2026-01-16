using AutoMapper;
using Macquarie.Handbook.Data;
using Macquarie.Handbook.Data.Course;
using Macquarie.Handbook.Data.Shared;
using Macquarie.Handbook.Data.Unit;
using Macquarie.Handbook.Data.Unit.Prerequisites;
using MQHandbookAPI.Models.Macquarie.Handbook.Data.Course;
using MQHandbookAPI.Models.Macquarie.Handbook.Data.Shared;
using MQHandbookAPI.Models.Macquarie.Handbook.Data.Unit;
using MQHandbookAPI.Models.Macquarie.Handbook.Data.Unit.Prerequisites;

namespace MQHandbookAPI.Models.Macquarie;

public class MacquarieDtoMappingProfile : Profile
{
    public MacquarieDtoMappingProfile() {
        AllowNullCollections = true;

        CreateMap<MacquarieUnit, UnitDto>();
        CreateMap<MacquarieUnitData, UnitDataDto>();
        CreateMap<LearningOutcome, LearningOutcomeDto>();
        CreateMap<EnrolmentRule, EnrolmentRuleDto>();
        CreateMap<Assessment, AssessmentDto>();
        CreateMap<UnitOffering, UnitOfferingDto>()
            .ForMember(dest => dest.Location, opt => opt.MapFrom(src => src.Location.Value))
            .ForMember(dest => dest.AcademicItem, opt => opt.MapFrom(src => src.AcademicItem.Value))
            .ForMember(dest => dest.AttendanceMode, opt => opt.MapFrom(src => src.AttendanceMode.Value))
            .ForMember(dest => dest.TeachingPeriod, opt => opt.MapFrom(src => src.TeachingPeriod.Value))
            .ForMember(dest => dest.StudyLevel, opt => opt.MapFrom(src => src.StudyLevel.Value));

        CreateMap<LearningActivity, LearningActivityDto>()
            .ForMember(dest => dest.Activity, opt => opt.MapFrom(src => src.Activity.Value));

        CreateMap<Requisite, RequisiteDto>()
            .ForMember(dest => dest.Type, opt => opt.MapFrom(src => src.RequisiteType.Value));

        CreateMap<ContainerRequisiteTemporaryName, RequisiteContainerDto>()
            .ForMember(dest => dest.Connector, opt => opt.MapFrom(src => src.ParentConnector.Value));

        CreateMap<MacquarieCourse, CourseDto>();

        CreateMap<MacquarieCourseData, CourseDataDto>()
            .ForMember(dest => dest.AqfLevel, opt => opt.MapFrom(src => src.AqfLevel.Label))
            .ForMember(dest => dest.GovtSpecialCourseType, opt => opt.MapFrom(src => src.GovtSpecialCourseType.Value))
            .ForMember(dest => dest.PartnerFaculty, opt => opt.MapFrom(src => src.PartnerFaculty.Value))
            .ForMember(dest => dest.ExclusivelyAnExitAward, opt => opt.MapFrom(src => src.ExclusivelyAnExitAward.Label))
            .ForMember(dest => dest.VolumeOfLearning, opt => opt.MapFrom(src => src.VolumeOfLearning.Label))
            .ForMember(dest => dest.CourseDurationInYears, opt => opt.MapFrom(src => src.CourseDurationInYears.Label))
            .ForMember(dest => dest.CourseValue, opt => opt.MapFrom(src => src.CourseValue.Value));

        CreateMap<MacquarieCurriculumStructureData, CurriculumStructureDataDto>();
        CreateMap<UnitGroupingContainer, UnitGroupingContainerDto>()
            .ForMember(dest => dest.ParentRecord, opt => opt.MapFrom(src => src.ParentRecord.Value))
            .ForMember(dest => dest.ParentRecord, opt => opt.MapFrom(src => src.ParentRecord.Value));

        CreateMap<Offering, OfferingDto>()
            .ForMember(dest => dest.AdmissionCalendar, opt => opt.MapFrom(src => src.AdmissionCalendar.Value))
            .ForMember(dest => dest.Location, opt => opt.MapFrom(src => src.Location.Value))
            .ForMember(dest => dest.Mode, opt => opt.MapFrom(src => src.Mode.Value))
            .ForMember(dest => dest.Status, opt => opt.MapFrom(src => src.Status.Value));

        CreateMap<AcademicItem, AcademicItemDto>()
            .ForMember(dest => dest.Type, opt => opt.MapFrom(src => src.Type.Label));

        CreateMap<OrgUnitData, OrgUnitDataDto>();
        CreateMap<Fee, FeeDto>()
            .ForMember(dest => dest.FeeType, opt => opt.MapFrom(src => src.FeeType.Value));

        CreateMap<CourseNote, CourseNoteDto>()
            .ForMember(dest => dest.Type, opt => opt.MapFrom(src => src.Type.Value));

        CreateMap<AdmissionRequirementPoint, AdmissionRequirementPointDto>()
           .ForMember(dest => dest.VolumeOfLearning, opt => opt.MapFrom(src => src.VolumeOfLearning.Label));

        CreateMap<LabelledValue, string>().ConvertUsing(src => src.Label);
        CreateMap<KeyValueIdType, string>().ConvertUsing(src => src.Value);

        CreateMap<MacquarieMaterialMetadata, MaterialMetadataDto>()
            .ForMember(dest => dest.AcademicOrganisation, opt => opt.MapFrom(src => src.AcademicOrganisation.Value))
            .ForMember(dest => dest.School, opt => opt.MapFrom(src => src.School.Value))
            .ForMember(dest => dest.Type, opt => opt.MapFrom(src => src.Type.Value))
            .ForMember(dest => dest.Status, opt => opt.MapFrom(src => src.Status.Value));
    }
}