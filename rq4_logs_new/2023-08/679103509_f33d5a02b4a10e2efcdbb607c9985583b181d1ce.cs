public ActionResult Search(string jabatanFilter, string lokasiFilter)
{
    var lamaranList = _context.Lamaran.ToList();

    if (!string.IsNullOrEmpty(jabatanFilter))
    {
        lamaranList = lamaranList.Where(l => l.Jabatan.ToLower().Contains(jabatanFilter.ToLower())).ToList();
    }

    if (!string.IsNullOrEmpty(lokasiFilter))
    {
        lamaranList = lamaranList.Where(l => l.Lokasi.ToLower().Contains(lokasiFilter.ToLower())).ToList();
    }

    ViewBag.JabatanFilter = jabatanFilter;
    ViewBag.LokasiFilter = lokasiFilter;

    return View("Search", lamaranList);
}