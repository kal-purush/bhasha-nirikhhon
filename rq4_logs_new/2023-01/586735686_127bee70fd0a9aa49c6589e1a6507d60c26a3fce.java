package tososomaru.wb.ads.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tososomaru.wb.ads.usecase.bids.*;
import tososomaru.wb.ads.usecase.bids.history.*;
import tososomaru.wb.ads.usecase.bids.history.impl.*;
import tososomaru.wb.ads.usecase.bids.impl.GetBidsByAdsTypeUseCase;
import tososomaru.wb.ads.usecase.bids.impl.GetCarouselBidsUseCase;
import tososomaru.wb.ads.usecase.bids.impl.GetCategoryBidsUseCase;
import tososomaru.wb.ads.usecase.bids.impl.GetSearchBidsUseCase;
import tososomaru.wb.ads.usecase.campaigns.*;
import tososomaru.wb.ads.usecase.campaigns.impl.*;
import tososomaru.wb.ads.usecase.companies.*;
import tososomaru.wb.ads.usecase.companies.impl.*;
import tososomaru.wb.ads.wbapi.WbApi;
import tososomaru.wb.ads.wbapi.WbMenuIdStore;

@Configuration
public class UseCaseConfiguration {

    @Bean
    public CategoryAdsToBidsMapper categoryAdsToBidsMapper() {
        return new CategoryAdsToBidsMapper();
    }

    @Bean
    public SearchAdsToBidsMapper searchAdsToBidsMapper() {
        return new SearchAdsToBidsMapper();
    }

    @Bean
    public GetBidsByAdsType getBidsByAdsType(
            GetCarouselBids getCarouselBids,
            GetCategoryBids getCategoryBids,
            GetSearchBids getSearchBids,
            AddBidRequestToHistory addBidRequestToHistory
    ) {
        return new GetBidsByAdsTypeUseCase(
                getCarouselBids,
                getCategoryBids,
                getSearchBids,
                addBidRequestToHistory
        );
    }

    @Bean
    public GetCarouselBids getCarouselBids(WbApi wbApi) {
        return new GetCarouselBidsUseCase(wbApi);
    }

    @Bean
    public GetCategoryBids getCategoryBids(
            CategoryAdsToBidsMapper categoryAdsToBidsMapper,
            WbApi wbApi,
            WbMenuIdStore wbMenuIdStore
    ) {
        return new GetCategoryBidsUseCase(
                categoryAdsToBidsMapper,
                wbApi,
                wbMenuIdStore
        );
    }

    @Bean
    public GetSearchBids getSearchBids(
            WbApi wbApi,
            SearchAdsToBidsMapper searchAdsToBidsMapper
    ) {
        return new GetSearchBidsUseCase(wbApi, searchAdsToBidsMapper);
    }

    @Bean
    public AddBidRequestToHistory addBidRequestToHistory(
            BidRequestsHistorySaver bidRequestsHistorySaver
    ) {
        return new AddBidRequestToHistoryUseCase(bidRequestsHistorySaver);
    }

    @Bean
    public GetAllBidRequestHistory getAllBidRequestHistory(
            BidRequestsHistoryExtractor bidRequestsHistoryExtractor
    ) {
        return new GetAllBidRequestHistoryUseCase(bidRequestsHistoryExtractor);
    }

    @Bean
    public GetRequestBidsFromHistoryById getRequestBidsFromHistoryById(
            BidRequestsHistoryExtractor bidRequestsHistoryExtractor
    ) {
        return new GetRequestBidsFromHistoryByIdUseCase(bidRequestsHistoryExtractor);
    }

    @Bean
    public GetTop5BidRequestHistory getTop5BidRequestHistory(
            BidRequestsHistoryExtractor bidRequestsHistoryExtractor
    ) {
        return new GetTop5BidRequestHistoryUseCase(bidRequestsHistoryExtractor);
    }

    @Bean
    public UpdateBidsFromHistory updateBidsFromHistory(
            GetBidsByAdsType getBidsByAdsType,
            GetRequestBidsFromHistoryById getRequestBidsFromHistoryById
    ) {
        return new UpdateBidsFromHistoryUseCase(
                getBidsByAdsType,
                getRequestBidsFromHistoryById
        );
    }

    @Bean public CreateCampaign createCampaign(
            CompanyExtractor companyExtractor,
            CampaignSaver campaignSaver
    ) {
        return new CreateCampaignUseCase(campaignSaver, companyExtractor);
    }

    @Bean public DeleteCampaign deleteCampaign(
            CampaignRemover campaignRemover
    ) {
      return new DeleteCampaignUseCase(campaignRemover);
    }

    @Bean public GetAllCampaigns getAllCampaigns(
            CampaignExtractor campaignExtractor
    ) {
        return new GetAllCampaignsUseCase(campaignExtractor);
    }

    @Bean public GetCampaignById getCampaignById(
            CampaignExtractor campaignExtractor
    ) {
        return new GetCampaignByIdUseCase(campaignExtractor);
    }

    @Bean public ResumeCampaign resumeCampaign(
            CampaignExtractor campaignExtractor,
            CampaignSaver campaignSaver
    ) {
        return new ResumeCampaignUseCase(
                campaignExtractor,
                campaignSaver
        );
    }

    @Bean public SuspendCampaign suspendCampaign(
       CampaignExtractor campaignExtractor,
       CampaignSaver campaignSaver
    ) {
        return new SuspendCampaignUseCase(
                campaignExtractor,
                campaignSaver
        );
    }

    @Bean public SynchronizeCampaignsUseCase synchronizeCampaignsUseCase(
        WbApi wbApi,
        CampaignExtractor campaignExtractor
    ) {
        return new SynchronizeCampaignsUseCase(
                wbApi, campaignExtractor
        );
    }

    @Bean public UpdateCampaignUseCase updateCampaignUseCase() {
        return new UpdateCampaignUseCase();
    }

    @Bean public AddCompany addCompany(CompanySaver companySaver) {
        return new AddCompanyUseCase(companySaver);
    }

    @Bean public ChangeCompanyName changeCompanyName(
            CompanyExtractor companyExtractor,
            CompanySaver companySaver
    ) {
        return new ChangeCompanyNameUseCase(
                companyExtractor,
                companySaver
        );
    }

    @Bean public ChangeCompanyAuth changeCompanyAuth(
            CompanyExtractor companyExtractor,
            CompanySaver companySaver
    ) {
        return new ChangeCompanyAuthUseCase(companyExtractor, companySaver);
    }

    @Bean public DeleteCompany deleteCompany(
            CompanyRemover companyRemover
    ) {
        return new DeleteCompanyUseCase(companyRemover);
    }

    @Bean public GetAllCompanies getAllCompanies(
            CompanyExtractor companyExtractor
    ) {
        return new GetAllCompaniesUseCase(companyExtractor);
    }

    @Bean public GetCompanyById getCompanyById(
            CompanyExtractor companyExtractor
    ) {
        return new GetCompanyByIdUseCase(companyExtractor);
    }
}