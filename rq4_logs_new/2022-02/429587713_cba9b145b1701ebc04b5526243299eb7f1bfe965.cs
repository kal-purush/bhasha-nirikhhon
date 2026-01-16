using ChessApiContract.Response;
using ChessApiContract;
using System.Text.Json;

namespace ChessApiClient
{
    public class ChessApiClient : IChessApiClient
    {
        private HttpClient _httpClient;

        public ChessApiClient(string baseAddress)
        {
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(baseAddress)
            };
        }

        private async Task<T?> GetResponseObject<T>(HttpResponseMessage response)
        {
            try
            {
                var stringContent = await response.Content.ReadAsStringAsync();
                return JsonSerializer.Deserialize<T>(stringContent, new JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                })!;
            }
            catch
            {
                return default(T);
            }
        }

        private async Task<T> PerformRequest<T>(HttpRequestMessage message) where T : IGameResponse<T>
        {
            try
            {
                var response = await _httpClient.SendAsync(message);
                if (response.IsSuccessStatusCode)
                {
                    return await GetResponseObject<T>(response) ?? T.RespondError("Could not interpret api response");
                }

                var errorResponse = await GetResponseObject<ErrorResponse>(response);
                if (errorResponse != null)
                {
                    return T.RespondError(errorResponse.Detail);
                }
            }
            catch (HttpRequestException ex) when ( ex.StatusCode is null ) 
            {
                return T.RespondError($"Could not connect to api at \"{message.RequestUri}\"");
            }

            return T.RespondError("Error while calling api");
        }

        public Task<NewGameResponse> NewGameAsync()
        {
            return PerformRequest<NewGameResponse>(new HttpRequestMessage(HttpMethod.Post, Calls.NewGame));
        }

        public Task<GameListResponse> GetGameListAsync()
        {
            return PerformRequest<GameListResponse>(new HttpRequestMessage(HttpMethod.Get, Calls.GameList));
        }

        public Task<GetGameResponse> GetGameAsync(Guid gameId)
        {
            return PerformRequest<GetGameResponse>(new HttpRequestMessage(HttpMethod.Get, $"{Calls.GameById}/{gameId}"));
        }

        public Task<MovePieceResponse> MovePieceAsync(Guid gameId, string fromCellName, string toCellName)
        {
            return PerformRequest<MovePieceResponse>(new HttpRequestMessage(HttpMethod.Put, $"/{Calls.MovePiece}/{gameId}/{fromCellName}/{toCellName}"));
        }

        public Task<AllowedMovesResponse> GetAllowedMovesAsync(Guid gameId, string pieceCellName)
        {
            return PerformRequest<AllowedMovesResponse>(new HttpRequestMessage(HttpMethod.Get, $"/{Calls.AllowedMoves}/{gameId}/{pieceCellName}"));
        }
    }
}