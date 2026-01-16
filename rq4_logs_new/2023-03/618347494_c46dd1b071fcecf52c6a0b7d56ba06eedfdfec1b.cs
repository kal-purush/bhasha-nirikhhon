using BuckingshireImporter.Services;
using FamilyHubs.ServiceDirectory.Shared.Dto;
using FamilyHubs.ServiceDirectory.Shared.Enums;
using PluginBase;

namespace BuckingshireImporter;

internal class BuckinghamshireMapper : BaseMapper
{
    public string Name => "Buckinghamshire Mapper";

    private readonly IBuckinghamshireClientService _buckinghamshireClientService;
    public BuckinghamshireMapper(IBuckinghamshireClientService buckinghamshireClientService, IOrganisationClientService organisationClientService, string adminAreaCode, string key, OrganisationWithServicesDto parentLA)
        : base(organisationClientService, adminAreaCode, parentLA, key)
    {
        _buckinghamshireClientService = buckinghamshireClientService;
    }

    public async Task AddOrUpdateServices()
    {
        const int maxRetry = 3;
        const int startPage = 1;
        int errors = 0;
        await CreateOrganisationDictionary();
        await CreateTaxonomyDictionary();
        BuckinghapshireService buckinghapshireService = await _buckinghamshireClientService.GetServicesByPage(startPage);
        int totalPages = buckinghapshireService.totalPages;
        foreach(var content in buckinghapshireService.content)
        {
            errors += await AddAndUpdateService(content);
        }
        Console.WriteLine($"Completed Page {startPage} of {totalPages} with {errors} errors");
        
        for (int i = startPage + 1; i <= totalPages; i++)
        {
            errors = 0;

            int retry = 0;
            while (retry < maxRetry)
            {
                try
                {
                    buckinghapshireService = await _buckinghamshireClientService.GetServicesByPage(i);
                    break;
                }
                catch 
                {
                    System.Threading.Thread.Sleep(1000);
                    retry++;
                    if (retry > maxRetry)
                    {
                        Console.WriteLine($"Failed to get page");
                        return;

                    }
                    Console.WriteLine($"Doing retry: {retry}");
                }
            }
            
            foreach (var content in buckinghapshireService.content)
            {
                errors += await AddAndUpdateService(content);
            }
            Console.WriteLine($"Completed Page {i} of {totalPages} with {errors} errors");

        }
        
    }

    private async Task<int> AddAndUpdateService(Content content)
    {
        List<string> errors = new List<string>();

        OrganisationWithServicesDto serviceDirectoryOrganisation = default!;

        bool newOrganisation = false;
        if (_dictOrganisations.ContainsKey($"{_adminAreaCode}{content.organisation.name}"))
        {
            serviceDirectoryOrganisation = _dictOrganisations[$"{_adminAreaCode}{content.organisation.name}"];
            //Get latest
            serviceDirectoryOrganisation = await _organisationClientService.GetOrganisationById(serviceDirectoryOrganisation.Id.ToString());
        }
        else
        {
            const OrganisationType organisationType = OrganisationType.VCFS;
            serviceDirectoryOrganisation = new OrganisationWithServicesDto
            {
                AdminAreaCode = _adminAreaCode,
                OrganisationType = organisationType,
                Name = content.organisation.name,
                Description = !string.IsNullOrEmpty(content.organisation.description) ? content.organisation.description : content.organisation.name,
                Logo = null,
                Uri = content.organisation.url,
                Url = content.organisation.url,
                Services = new List<ServiceDto>()
            };

            newOrganisation = true;
        }

        var serviceOwnerReferenceId = $"{_adminAreaCode.Replace("E", "")}{content.id}";

        ServiceDto? existingService = serviceDirectoryOrganisation.Services.FirstOrDefault(s => s.ServiceOwnerReferenceId == serviceOwnerReferenceId);

        ServiceDto serviceDto = new ServiceDto()
        {
            Id = (existingService != null) ? existingService.Id : 0,
            OrganisationId = serviceDirectoryOrganisation.Id,
            ServiceType = ServiceType.InformationSharing,
            ServiceOwnerReferenceId = serviceOwnerReferenceId,
            Name = content.name,
            Description = content.description,
            Accreditations = null,
            AssuredDate = null,
            AttendingAccess = AttendingAccessType.NotSet,
            AttendingType = AttendingType.NotSet,
            DeliverableType = DeliverableType.NotSet,
            Status = StringToEnum.ConvertServiceStatusType(content.status),
            Fees = null,
            CanFamilyChooseDeliveryLocation = false,
            Eligibilities = GetEligibilityDtos(content, existingService),
            CostOptions = GetCostOptionDtos(content.cost_options, existingService),
            RegularSchedules = GetRegularSchedules(content.regular_schedules, existingService, null),
            Contacts = GetContactDtos(content.contacts, existingService),
            Taxonomies = await GetServiceTaxonomies(content.taxonomies, existingService),
            Locations = GetLocations(content.locations, existingService),

        };

        errors = await AddOrUpdateDirectoryService(newOrganisation, serviceDirectoryOrganisation, serviceDto, serviceOwnerReferenceId, errors);

        foreach (string error in errors)
        {
            Console.WriteLine(error);
        }

        return errors.Count;

    }

    private List<EligibilityDto> GetEligibilityDtos(Content content, ServiceDto? existingService)
    {
        List<EligibilityDto> listEligibilityDto = new List<EligibilityDto>();

        var newEligibility = new EligibilityDto
        {
            MinimumAge = content.min_age != null ? content.min_age.Value : 0,
            MaximumAge = content.max_age != null ? content.max_age.Value : 0,
            EligibilityType = EligibilityType.NotSet
        };

        if (existingService != null)
        {
            var existingItem = existingService.Eligibilities.FirstOrDefault(x => x.EligibilityType == newEligibility.EligibilityType
                         && x.MinimumAge == newEligibility.MinimumAge
                         && x.MaximumAge == newEligibility.MaximumAge);
            if (existingItem != null)
            {
                listEligibilityDto.Add(existingItem);
            }
        }

        listEligibilityDto.Add(newEligibility);

        return listEligibilityDto;
    }

    private List<CostOptionDto> GetCostOptionDtos(CostOptions[] costOptions, ServiceDto? existingService)
    {
        if (costOptions == null || !costOptions.Any())
        {
            return new List<CostOptionDto>();
        }

        List<CostOptionDto> listCostOptionDto = new List<CostOptionDto>();

        foreach (CostOptions costOption in costOptions)
        {
            if (string.IsNullOrEmpty(costOption.cost_type))
                continue;

            decimal.TryParse(costOption.amount, out decimal amount);

            var newCostOption = new CostOptionDto
            {
                AmountDescription = costOption.cost_type,
                Amount = amount,
                Option = costOption.option,
            };

            if (existingService != null)
            {
                var existingItem = existingService.CostOptions.FirstOrDefault(x => x.AmountDescription == newCostOption.AmountDescription
                             && x.Amount == newCostOption.Amount
                             && x.Option == newCostOption.Option
                             && x.ValidFrom == newCostOption.ValidFrom
                             && x.ValidTo == newCostOption.ValidTo);
                if (existingItem != null)
                {
                    listCostOptionDto.Add(existingItem);
                    continue;
                }
            }

            listCostOptionDto.Add(newCostOption);
        }

        return listCostOptionDto;
    }

    private List<RegularScheduleDto> GetRegularSchedules(RegularSchedule[] regularSchedules, ServiceDto? existingService, long? loactionid)
    {
        if (regularSchedules == null || !regularSchedules.Any())
        {
            return new List<RegularScheduleDto>();
        }

        List<RegularScheduleDto> listRegularScheduleDto = new List<RegularScheduleDto>();

        foreach (RegularSchedule regularSchedule in regularSchedules)
        {
            var regularScheduleItem = new RegularScheduleDto
            {
                OpensAt = Helper.GetDateFromString(regularSchedule.opens_at),
                ClosesAt = Helper.GetDateFromString(regularSchedule.closes_at),
                Freq = FrequencyType.NotSet,
                ByDay = regularSchedule.weekday,
            };


            if (existingService != null)
            {
                RegularScheduleDto existingItem = listRegularScheduleDto.FirstOrDefault(x => x.OpensAt == regularScheduleItem.OpensAt
                                               && x.ClosesAt == regularScheduleItem.ClosesAt
                                               && x.ValidFrom == regularScheduleItem.ValidFrom
                                               && x.ValidTo == regularScheduleItem.ValidTo
                                               && x.DtStart == regularScheduleItem.DtStart
                                               && x.Freq == regularScheduleItem.Freq
                                               && x.Interval == regularScheduleItem.Interval
                                               && x.ByDay == regularScheduleItem.ByDay
                                               && x.ByMonthDay == regularScheduleItem.ByMonthDay
                                               && x.Description == regularScheduleItem.Description
                                               ) ?? default!;
                if (existingItem != null)
                {
                    listRegularScheduleDto.Add(existingItem);
                    continue;
                }
            }

            listRegularScheduleDto.Add(regularScheduleItem);
        }

        return listRegularScheduleDto;
    }

    private List<ContactDto> GetContactDtos(Contact[] contacts, ServiceDto? existingService)
    {
        if (!contacts.Any())
        {
            return new List<ContactDto>();
        }

        var list = new List<ContactDto>();
        foreach (var contact in contacts)
        {
           if (contact == null || contact.phone == null) { continue; }

            var newContact = new ContactDto
            {
                Title = contact?.title ?? default!,
                Email = contact?.email ?? default!,
                Telephone = contact?.phone ?? default!,
                TextPhone = contact?.phone ?? default!,
            };

            if (existingService != null)
            {
                ContactDto existingItem = existingService.Contacts.FirstOrDefault(x => x.Name == newContact.Title && x.Telephone == newContact.Telephone && x.Email == newContact.Email) ?? default!;
                if (existingItem != null)
                {
                    list.Add(existingItem);
                    continue;
                }
            }

            list.Add(newContact);
        }

        return list;
    }

    private async Task<List<TaxonomyDto>> GetServiceTaxonomies(Taxonomy[] serviceTaxonomies, ServiceDto? existingService)
    {
        if (serviceTaxonomies == null || !serviceTaxonomies.Any())
        {
            return new List<TaxonomyDto>();
        }

        List<TaxonomyDto> listTaxonomyDto = new List<TaxonomyDto>();

        foreach (Taxonomy taxonomy in serviceTaxonomies)
        {
            TaxonomyDto taxonomyDto = new TaxonomyDto
            {
                Name = taxonomy.name,
                TaxonomyType = TaxonomyType.ServiceCategory
            };

            if (!_dictTaxonomies.ContainsKey(taxonomyDto.Name.ToLower()))
            {
                long result = await _organisationClientService.CreateTaxonomy(taxonomyDto);
                if (result > 0)
                {
                    _dictTaxonomies[taxonomyDto.Name.ToLower()] = taxonomyDto;
                }

            }

            if (existingService != null)
            {
                TaxonomyDto existing = existingService.Taxonomies.FirstOrDefault(x => x.Name.ToLower() == taxonomyDto.Name.ToLower()
                                                                                && x.TaxonomyType == taxonomyDto.TaxonomyType) ?? default!;

                if (existing != null)
                {
                    listTaxonomyDto.Add(existing);
                    continue;
                }
            }

            listTaxonomyDto.Add(taxonomyDto);
        }

        return listTaxonomyDto;
    }

    private List<LocationDto> GetLocations(Location[] locations, ServiceDto? existingService)
    {
        if (locations == null || !locations.Any())
        {
            return new List<LocationDto>();
        }

        List<LocationDto> listLocationDto = new List<LocationDto>();

        foreach (Location location in locations)
        {
            if (location == null)
            {
                continue;
            }

            var newLocation = new LocationDto
            {
                LocationType = LocationType.NotSet,
                Name = location.name ?? default!,
                Description = null,
                Latitude = location.geometry.coordinates[1],
                Longitude = location.geometry.coordinates[0],

                Address1 = !string.IsNullOrEmpty(location.address_1) ? location.address_1 : " ",
                City = !string.IsNullOrEmpty(location.city) ? location.city : " ",
                PostCode = !string.IsNullOrEmpty(location.postal_code) ? location.postal_code : " ",
                Country = !string.IsNullOrEmpty(location.country) ? location.country : "United Kingdom",
                StateProvince = !string.IsNullOrEmpty(location.state_province) ? location.state_province : " ",
            };

            if (existingService != null)
            {
                LocationDto existingItem = existingService.Locations.FirstOrDefault(x => x.Name == newLocation.Name
                                        && x.Description == newLocation.Description
                                        && x.Latitude == newLocation.Latitude
                                        && x.Longitude == newLocation.Longitude
                                        ) ?? default!;
                if (existingItem != null)
                {
                    listLocationDto.Add(existingItem);
                    continue;
                }
            }

            listLocationDto.Add(newLocation);
        }

        return listLocationDto;
    }
}