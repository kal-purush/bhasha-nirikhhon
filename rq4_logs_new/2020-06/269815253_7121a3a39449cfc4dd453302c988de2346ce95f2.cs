using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Quasar.Places.Application.Web.Commands;
using Quasar.Places.Application.Web.Data;
using Quasar.Places.Application.Web.Queries.Places;

namespace Quasar.Places.Application.Web.Controllers
{
	[Route("places")]
	public class PlaceController : ControllerBase
	{
		private readonly IPlacesData _placesData;

		/// <inheritdoc />
		public PlaceController(IPlacesData placesData)
		{
			_placesData = placesData ?? throw new ArgumentNullException(nameof(placesData));
		}

		[HttpGet(Name = "GetPlaces")]
		[ProducesResponseType(statusCode: 200, type: typeof(List<Place>))]
		public async Task<IActionResult> Get(CancellationToken cancellationToken)
		{
			IEnumerable<Place> places = await _placesData.GetAsync(cancellationToken);
			
			return this.Ok(places);
		}

		[HttpGet("{chatId}", Name = "GetChat")]
		[ProducesResponseType(statusCode: 200, type: typeof(PlaceDetails))]
		public async Task<IActionResult> Get(Guid chatId, CancellationToken cancellationToken)
		{
			PlaceDetails chat = await _placesData.GetByIdAsync(chatId, cancellationToken);
			
			return this.Ok(chat);
		}

		[HttpPost(Name = "PostPlace")]
		public async Task<IActionResult> Post([FromBody] CreateDirectPlaceCommand command, CancellationToken cancellationToken)
		{
			await _placesData.AddAsync(command, cancellationToken);

			return this.Ok();
		}

		[HttpPut(Name = "PutPlace")]
		public async Task<IActionResult> Put([FromBody] ChangePlacesInfoCommand command, CancellationToken cancellationToken)
		{
			PlaceDetails place = await _placesData.GetByIdAsync(command.PlaceId, cancellationToken);

			if (place == null)
			{
				return this.NotFound(new ProblemDetails()
				{
					Detail = $"Place with id {command.PlaceId} not found."
				});
			}
			
			await _placesData.UpdateAsync(command, cancellationToken);

			return this.Ok();
		}

		[HttpDelete("{placeId}", Name = "DeletePlace")]
		public async Task<IActionResult> Delete([FromRoute] Guid placeId, CancellationToken cancellationToken)
		{
			PlaceDetails chat = await _placesData.GetByIdAsync(placeId, cancellationToken);

			if (chat == null)
			{
				return this.NotFound(new ProblemDetails()
				{
					Detail = $"Chat with id {placeId} not found."
				});
			}
			
			await _placesData.RemoveAsync(placeId, cancellationToken);

			return this.Ok();
		}
	}
}