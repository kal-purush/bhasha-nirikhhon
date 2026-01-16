using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Web;
using System.Web.Mvc;
using WebTorrent.Domain.Services.Torrent;

namespace WebTorrent.WebApp.Controllers
{
    public class DownloadController : Common.ControllerBase
    {
        public FileResult Get(int id)
        {
            var zippedFile = TorrentEngine.Current.GetZippedFile(id);
            var torrentDto = TorrentEngine.Current.GetTorrentById(id);
            return File(zippedFile, "application/zip", string.Format("{0}.zip", torrentDto.Name));
        }
    }
}