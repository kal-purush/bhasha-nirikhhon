using System.Net.Mime;
using GiftSuggester.Core.Gifts.Models;
using GiftSuggester.Core.Gifts.Services;
using GiftSuggester.Web.Gifts.Models;
using Microsoft.AspNetCore.Mvc;

namespace GiftSuggester.Web.Gifts.Controllers;

[ApiController]
[Route("gifts")]
[Produces(MediaTypeNames.Application.Json)]
public class GiftController
{
    private readonly IGiftService _giftService;

    public GiftController(IGiftService giftService)
    {
        _giftService = giftService;
    }

    [HttpPost]
    public Task AddAsync(GiftCreationRequest creationRequest, CancellationToken cancellationToken)
    {
        return _giftService.AddAsync(
            new GiftCreationModel
            {
                Name = creationRequest.Name,
                GroupId = creationRequest.GroupId,
                PresenterId = creationRequest.PresenterId,
                RecipientId = creationRequest.RecipientId
            }, cancellationToken);
    }

    [HttpGet("getAllByPresenterId/{id:guid}")]
    public async Task<List<GiftResponse>> GetAllByPresenterIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var gifts = await _giftService.GetAllByPresenterIdAsync(id, cancellationToken);

        return gifts.Select(
                gift => new GiftResponse
                {
                    Id = gift.Id,
                    Name = gift.Name,
                    GroupId = gift.GroupId,
                    PresenterId = gift.PresenterId,
                    RecipientId = gift.RecipientId
                })
            .ToList();
    }

    [HttpGet("getAllByRecipientIdAsync/{id:guid}")]
    public async Task<List<GiftResponse>> GetAllByRecipientIdAsync(Guid id, CancellationToken cancellationToken)
    {
        var gifts = await _giftService.GetAllByRecipientIdAsync(id, cancellationToken);

        return gifts.Select(
                gift => new GiftResponse
                {
                    Id = gift.Id,
                    Name = gift.Name,
                    GroupId = gift.GroupId,
                    PresenterId = gift.PresenterId,
                    RecipientId = gift.RecipientId
                })
            .ToList();
    }

    [HttpPut]
    public Task UpdateAsync(GiftUpdateRequest updateRequest, CancellationToken cancellationToken)
    {
        return _giftService.UpdateAsync(
            new Gift
            {
                Id = updateRequest.Id,
                Name = updateRequest.Name,
                GroupId = updateRequest.GroupId,
                PresenterId = updateRequest.PresenterId,
                RecipientId = updateRequest.RecipientId
            }, cancellationToken);
    }

    [HttpDelete("{id:guid}")]
    public Task RemoveByIdAsync(Guid id, CancellationToken cancellationToken)
    {
        return _giftService.RemoveByIdAsync(id, cancellationToken);
    }
}