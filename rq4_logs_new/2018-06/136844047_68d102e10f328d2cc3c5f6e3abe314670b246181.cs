using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

using Microsoft.EntityFrameworkCore;
using AspNetCoreIntro.Models;

namespace AspNetCoreIntro.Controllers {

  public class ApiController : Controller {

    private Context Db;
    private ApiProxier Proxy;

    public ApiController(ApiProxier proxy, Context context) {
      Proxy = proxy;
      Db = context;
    }

    [HttpPost] // form-data
    [Route("/api/search")]
    // public IActionResult SearchForSong(string song) {
    async public Task<JsonResult> Search(string searchText) {
      var Url = $"search?q={searchText}";
      var Client = Proxy.Configure("Genius", Url);
      var Data = await Proxy.Get(Client);
      return Json(Data);
      // Genius: Search for songs
      // 1. Make async call to API
      // 2. Filter through data with any Db info
      //    e.g. Your likes, other user's likes
      // 3. Send data to other route via TempData
      // 4. Render data on other route into ViewBag
      // return RedirectToAction("ShowSongResults", "Artist");
    }

    [Route("api/artists/{artistId}")]
    // async public Task<IActionResult> GetArtistInfo(string artistId) {
    async public Task<JsonResult> GetArtistBio(string artistId) {
      var Url = $"/artists/{artistId}";
      var Client = Proxy.Configure("Genius", Url);
      var Data = await Proxy.Get(Client);
      return Json(Data);
      // Genius: Get artist info (i.e. their songs)
      // Filter through data with any Db info (e.g. likes)
      // return RedirectToAction("ShowArtistPage", "Artist");
    }

    [Route("api/artists/{artistId}/songs&sort=popularity")]
    // async public Task<IActionResult> GetArtistInfo(string artistId) {
    async public Task<JsonResult> GetArtistSongs(string artistId) {
      var Url = $"/artists/{artistId}/songs";
      var Client = Proxy.Configure("Genius", Url);
      var Data = await Proxy.Get(Client);
      return Json(Data);
      // Genius: Get artist info (i.e. their songs)
      // Filter through data with any Db info (e.g. likes)
      // return RedirectToAction("ShowArtistPage", "Artist");
    }

    // Like: Check DB for artist; if not, write to DB
    // Like: Check DB for song; if not, write to DB

    // Create associations with users
    // - Many-to-many users & artists
    // - Many-to-many users & songs
    // - Many-to-many users as friends (self-join)

  }

}