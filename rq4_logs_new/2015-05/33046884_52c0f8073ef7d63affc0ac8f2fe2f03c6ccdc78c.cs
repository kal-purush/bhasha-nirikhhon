// ****************************************
// Assembly : NetflixRouletteSharp
// File     : TorrentDescriptionResponse.cs
// Author   : Alan Wright
// ****************************************
// Created  : 05/09/2015
// ****************************************

using Newtonsoft.Json;

namespace StrikeApiSharp.Contracts.Responses
{
    public class TorrentDescriptionResponse
    {
        [JsonProperty("message")]
        public string Description { get; private set; }
    }
}