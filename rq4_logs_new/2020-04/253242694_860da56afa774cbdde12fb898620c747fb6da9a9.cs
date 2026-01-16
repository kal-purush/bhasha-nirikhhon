using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using MAVN.Service.SmartVouchers.Client.Models.Requests;
using MAVN.Service.SmartVouchers.Client.Models.Responses;
using MAVN.Service.SmartVouchers.Client.Models.Responses.Enums;
using Refit;

namespace MAVN.Service.SmartVouchers.Client
{
    /// <summary>
    /// Vouchers campaigns API interface.
    /// </summary>
    [PublicAPI]
    public interface IVoucherCampaignsApi
    {
        /// <summary>
        /// Returns list of Campaigns.
        /// </summary>
        /// <returns>CampaignListModel</returns>
        [Get("/api/campaigns")]
        Task<PaginatedVoucherCampaignsListResponseModel> GetAsync(VoucherCampaignsPaginationRequestModel request);

        /// <summary>
        /// Returns a Campaign by campaignId.
        /// </summary>
        /// <returns>CampaignResponseModel</returns>
        [Get("/api/campaigns/{campaignId}")]
        Task<VoucherCampaignDetailsResponseModel> GetByIdAsync(Guid campaignId);

        /// <summary>
        /// Returns a list of Campaigns by passed campaignIds.
        /// </summary>
        /// <returns>CampaignsInfoListResponseModel</returns>
        [Get("/api/campaigns/ids")]
        Task<VoucherCampaignsListResponseModel> GetCampaignsByIds([Query(CollectionFormat.Multi)] Guid[] voicherCampaignsIds);

        /// <summary>
        /// Adds new Campaign (with conditions).
        /// </summary>
        /// <param name="model">The model that describes instrument.</param>
        [Post("/api/campaigns")]
        Task<VoucherCampaignServiceErrorCodes> CreateAsync([Body] VoucherCampaignCreateModel model);

        /// <summary>
        /// Updates existing Campaign (with conditions).
        /// </summary>
        [Put("/api/campaigns")]
        Task<VoucherCampaignServiceErrorCodes> UpdateAsync([Body] VoucherCampaignEditModel model);

        /// <summary>
        /// Deletes Campaign by identification.
        /// </summary>
        [Delete("/api/campaigns/{campaignId}")]
        Task<VoucherCampaignServiceErrorCodes> DeleteAsync(Guid campaignId);

        /// <summary>
        /// Adds new Campaign's content image
        /// </summary>
        /// <param name="model">The model that describes the file.</param>
        [Post("/api/campaigns/image")]
        Task<VoucherCampaignServiceErrorCodes> SetImage([Body] CampaignImageFileRequest model);
    }
}