using PortFolio2017.Models;
using PortFolio2017.Services;
using PortFolio2017.ViewModels;
using System.Collections.Generic;
using System.Linq;

namespace PortFolio2017.ViewModelBuilder
{
    public class BaseModelBuilder
    {
        #region GetList
        internal static AddressListViewModel GetAddress(IBaseService BaseService)
        {
            return new AddressListViewModel{
                Addresses = BaseService.GetAddresses().Select(x => new AddressViewModel {
                            City = x.City,
                            Country = x.Country,
                            HouseNumber = x.HouseNumber,
                            RoadNumber = x.RoadNumber
                        }).ToList()
            };
        }
        internal static SocialIconsViewModel GetSocialLinksForSidebar(IBaseService BaseService)
        {
            return new SocialIconsViewModel{
                Icons = BaseService.GetAllSocialServices().
                        Where(x => x.SocialLinkType == Enums.SocialLinkType.Sidebar).
                        Select(x => new SocialViewModel {
                            Icon = x.Icon,
                            Name = x.Name,
                            Url = x.Url
                        }).ToList()
            };
        }
        internal static HeadersViewModel GetHeaders(IBaseService BaseService)
        {
            return new HeadersViewModel {
                Headers = BaseService.GetAllHeaders().Select(x => x.Text).
                ToList()
            };
        }
        internal static MottosViewModel GetMottos(IBaseService BaseService)
        {
            return new MottosViewModel { Mottos= BaseService.GetAllMottos().Select(x=>x.Text).ToList() };
        }
        internal static GeneralSkillsList GetGeneralSkills(IBaseService BaseService)
        {
            return new GeneralSkillsList {
                Skills = BaseService.GetAllGeneralSkills().
                Select(x => new GeneralSkillViewModel {
                    Name = x.Text,
                    Percentage = x.Percentage
                }).ToList()
            };
        }
        internal static WorkHistoryViewModel GetWorkHistory(IBaseService BaseService)
        {
            return new WorkHistoryViewModel {
                Works = BaseService.GetAllWorks().Select(
                    x => new WorkViewModel {
                            Title = x.Title,
                            Description = x.Description,
                            FromDate = x.FromDate,
                            ToDate = x.ToDate,
                            CurrentlyEmployed = x.CurrentlyEmployed,
                            Url = x.Url,
                            UrlText = x.UrlText
                    }).ToList()
            };
        }
        internal static EducationHistoryViewModel GetEducationHistory(IBaseService BaseService)
        {
            return new EducationHistoryViewModel
            {
                Educations = BaseService.GetAllEducationDetails().Select(
                    x => new EducationViewModel
                    {
                        Title = x.Title,
                        Description = x.Description,
                        FromDate = x.FromDate,
                        ToDate = x.ToDate,
                        CurrentlyEnrolled = x.CurrentlyEnrolled,
                        Url = x.Url,
                        UrlText = x.UrlText
                    }).ToList()
            };
        }
        internal static PublicationListViewModel GetAllPublications(IBaseService BaseService)
        {
            return new PublicationListViewModel
            {
                Publications = BaseService.GetAllPublications().ToList().
                                Select(x => new PublicationViewModel {
                                    Name = x.Name,
                                    Description = x.Description,
                                    Url = x.Url,
                                    UrlText = x.UrlText,
                                    Authors = BaseService.GetAuthorsOfAPublication(x.Id).Select(y => y.Name).ToList()
                                }).ToList()
            };
        }
        internal static ExpertiseListViewModel GetAllExpertise(IBaseService BaseService)
        {
            return new ExpertiseListViewModel
            {
                Expertise = BaseService.GetAllExpertise().Select(x => new ExpertiseViewModel {
                    Title = x.Title,
                    Details = x.Details
                }).ToList()
            };
        }
        internal static SpecialSkillsList GetSpecialSkillsWithChart(IBaseService BaseService)
        {
            return new SpecialSkillsList
            {
                Skills = BaseService.GetAllSpecialSkills().Where(x => x.DisplayType == Enums.DisplayType.Chart)
                         .Select(x => new SpecialSkillsViewModel {
                             Title = x.Title,
                             Percentage = x.Percentage
                         }).ToList()
            };
        }
        internal static SpecialSkillsList GetSpecialSkillsWithBar(IBaseService BaseService)
        {
            return new SpecialSkillsList
            {
                Skills = BaseService.GetAllSpecialSkills().Where(x => x.DisplayType == Enums.DisplayType.ProgressBar)
                         .Select(x => new SpecialSkillsViewModel
                         {
                             Title = x.Title,
                             Percentage = x.Percentage
                         }).ToList()
            };
        }
        internal static SocialIconsViewModel GetSocialLinksForFooter(IBaseService BaseService)
        {
            return new SocialIconsViewModel
            {
                Icons = BaseService.GetAllSocialServices().
                        Where(x => x.SocialLinkType == Enums.SocialLinkType.Footer).
                        Select(x => new SocialViewModel
                        {
                            Icon = x.Icon,
                            Name = x.Name,
                            Url = x.Url
                        }).ToList()
            };
        }
        internal static AddressListViewModel GetAddresses(IBaseService BaseService)
        {
            return new AddressListViewModel
            {
                Addresses = BaseService.GetAddresses().Select(x => new AddressViewModel {
                    RoadNumber = x.RoadNumber,
                    HouseNumber = x.HouseNumber,
                    City = x.City,
                    Country = x.Country
                }).ToList()
            };
        }
        internal static PhoneViewModel GetPhones(IBaseService BaseService)
        {
            return new PhoneViewModel
            {
                Phones = BaseService.GetAllPhones().Select(x => x.PhoneNumber).ToList()
            };
        }
        internal static EmailViewModel GetEmails(IBaseService BaseService)
        {
            return new EmailViewModel
            {
                Emails = BaseService.GetAllEmails().Select(x => x.EmailAddress).ToList()
            };
        }
        #endregion
    }
}