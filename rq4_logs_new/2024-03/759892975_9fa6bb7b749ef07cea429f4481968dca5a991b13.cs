using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using YouCode.BE;
using YouCode.BL;

namespace YouCode.GUI.Controllers;

public class ReportController : Controller
{
    ReportBL reportBL = new ReportBL();

    public ActionResult Index()
    {
        return View();
    }

    //
    public IActionResult Create()
    {
        ViewBag.Error = "";
        return View();
    }

    // 
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(Report report)
    {
        try
        {
            int result = await reportBL.CreateAsync(report);
            return RedirectToAction(nameof(Index));
        }
        catch(Exception ex)
        {
            ViewBag.Error = ex.Message;
            return View(report);
        }
    }

    // 
    public async Task<IActionResult> Edit(int id)
    {
        var report = await reportBL.GetByIdAsync(new Report {Id = id});
        return View(report);
    }

    // 
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, Report report)
    {
        try
        {
            int result = await reportBL.UpdateAsync(report);
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            ViewBag.Error = ex.Message;
            return View();
        }
    }

    // 
    public async Task<IActionResult> Delete(int id)
    {
        var report = await reportBL.GetByIdAsync(new Report { Id = id });
        return View(report);
    }

    // 
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Delete(int id, Report report)
    {
        try
        {
            int result = await reportBL.DeleteAsync(report);
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            ViewBag.Error = ex.Message;
            return View(report);
        }
    }
}