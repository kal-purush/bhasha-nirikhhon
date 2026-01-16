using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Univalle.Fie.Sistemas.UniBook.AppUnibookRegisterMvc.Models;
using System.ServiceModel;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace AppUnibookRegisterMvc.Controllers
{
    public class CareerController : Controller
    {
        private readonly ILogger _logger;
        IConfiguration _iconfiguration;
        private ILogger<CareerController> logger;

        public CareerController(ILogger<CareerController> logger, IConfiguration iconfiguration)
        {
            _logger = logger;
            this.logger = logger;
            _iconfiguration = iconfiguration;
        }

        public List<CareerModel> CareerModel
        {
            get
            {
                WebCareerService.CareerDto careers = null;
                try
                {
                    var url = _iconfiguration.GetValue<string>("WebServices:Career:WebCareerService");
                    //Create instance of SOAP client
                    WebCareerService.WebCareerServiceSoapClient soapClient = new WebCareerService.WebCareerServiceSoapClient(new BasicHttpBinding(BasicHttpSecurityMode.None), new EndpointAddress(url));
                    careers = soapClient.Get();
                }
                catch (System.Net.Http.HttpRequestException ex)
                {
                    _logger.LogCritical(ex.Message);
                    _logger.LogCritical(ex.StackTrace);
                }
                catch (Exception ex)
                {
                    _logger.LogCritical(ex.Message);
                    _logger.LogCritical(ex.StackTrace);
                }

                CareerModel career = new CareerModel
                {
                    CareerId = careers.CareerId,
                    Deleted = careers.Deleted,
                    Name = careers.Name,
                    Faculty = new FacultyModel() { FacultyId = careers.Faculty.FacultyId, Name = careers.Faculty.Name, Deleted = careers.Faculty.Deleted }
                };
                List<CareerModel> list = new List<CareerModel>();
                list.Add(career);
                return list;
            }
        }
        // GET: Career
        public ActionResult Index()
        {
            //ViewData["Index"] = _localizer["Index"];
            //ViewData["Edit"] = _localizer["Edit"];
            //ViewData["Details"] = _localizer["Details"];
            //ViewData["Delete"] = _localizer["Delete"];

            List<CareerModel> model = null;
            try
            {
                model = CareerModel;
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }

            return View(model);
        }

        // GET: Career/Details/5
        public ActionResult Details(int id)
        {
            //ViewData["Person"] = _localizer["Person"];
            //ViewData["Edit"] = _localizer["Edit"];
            //ViewData["Details"] = _localizer["Details"];
            //ViewData["BackToList"] = _localizer["BackToList"];

            WebCareerService.CareerDto careerDto = null;
            try { 
           
                var url = _iconfiguration.GetValue<string>("WebServices:Career:WebCareerService");
                WebCareerService.WebCareerServiceSoapClient soapClient = new WebCareerService.WebCareerServiceSoapClient(new BasicHttpBinding(BasicHttpSecurityMode.None), new EndpointAddress(url));
                careerDto = soapClient.GetId(id);
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }

            CareerModel careerModel = new CareerModel()
            {
                CareerId = careerDto.CareerId,
                Deleted = careerDto.Deleted,
                Name = careerDto.Name,
                Faculty = new FacultyModel() { FacultyId = careerDto.Faculty.FacultyId, Name = careerDto.Faculty.Name, Deleted = careerDto.Faculty.Deleted }
            };
            return View(careerModel);
        }

        // GET: Career/Create
        public ActionResult Create()
        {
            try
            {

                CareerController careerController = new CareerController(logger, _iconfiguration);
                ViewBag.ListCareer = new Microsoft.AspNetCore.Mvc.Rendering.SelectList(
                            (
                                from career in careerController.CareerModel
                                select new SelectListItem
                                {
                                    Text = career.Name,
                                    Value = career.CareerId.ToString()
                                }
                            )
                            , "Value", "Text");
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }
            return View();
        }

        // POST: Career/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(CareerModel careerModel)
        {
            try
            {
                var url = _iconfiguration.GetValue<string>("WebServices:Career:WebCareerService");

                WebCareerService.WebCareerServiceSoapClient soapClient = new WebCareerService.WebCareerServiceSoapClient(new BasicHttpBinding(BasicHttpSecurityMode.None), new EndpointAddress(url));
                // TODO: Add insert logic here
                WebCareerService.CareerDto career = new WebCareerService.CareerDto()
                {
                    Deleted = false,
                    Name = careerModel.Name,
                    Faculty = new WebCareerService.FacultyDto() { FacultyId = careerModel.Faculty.FacultyId }
                };
                soapClient.Insert(career);
                return RedirectToAction(nameof(Index));
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
                return View();
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
                return View();
            }
        }

        // GET: Career/Edit/5
        public ActionResult Edit(int id)
        {
            //ViewData["Edit"] = _localizer["Edit"];
            //ViewData["Person"] = _localizer["Person"];
            //ViewData["Save"] = _localizer["Save"];
            //ViewData["BackToList"] = _localizer["BackToList"];

            CareerController careerController = new CareerController(logger, _iconfiguration);
            WebCareerService.CareerDto caeerDto = null;
            try
            {
                ViewBag.ListCareer = new Microsoft.AspNetCore.Mvc.Rendering.SelectList(
                      (
                          from career in careerController.CareerModel
                          select new SelectListItem
                          {
                              Text = career.Name,
                              Value = career.CareerId.ToString()
                          }
                      )
                      , "Value", "Text");

                var url = _iconfiguration.GetValue<string>("WebServices:Career:WebCareerService");
                WebCareerService.WebCareerServiceSoapClient soapClient = new WebCareerService.WebCareerServiceSoapClient(new BasicHttpBinding(BasicHttpSecurityMode.None), new EndpointAddress(url));
                caeerDto = soapClient.GetId(id);
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
            }

            CareerModel careerModel = new CareerModel()
            {
                CareerId = caeerDto.CareerId,
                Deleted = caeerDto.Deleted,
                Name = caeerDto.Name,
                Faculty = new FacultyModel() { FacultyId = caeerDto.Faculty.FacultyId, Name = caeerDto.Faculty.Name, Deleted = caeerDto.Faculty.Deleted }
            };

            return View(careerModel);
        }

        // POST: Career/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, CareerModel careerModel)
        {
            try
            {
                var url = _iconfiguration.GetValue<string>("WebServices:Career:WebCareerService");
                // TODO: Add update logic here
                WebCareerService.WebCareerServiceSoapClient soapClient = new WebCareerService.WebCareerServiceSoapClient(new BasicHttpBinding(BasicHttpSecurityMode.None), new EndpointAddress(url));
                // TODO: Add insert logic here
                WebCareerService.CareerDto role = new WebCareerService.CareerDto()
                {
                    CareerId = careerModel.CareerId,
                    Deleted = careerModel.Deleted,
                    Name = careerModel.Name,
                    Faculty = new WebCareerService.FacultyDto() { FacultyId = careerModel.Faculty.FacultyId }
                };
                soapClient.Update(role);

                return RedirectToAction(nameof(Index));
            }
            catch (System.Net.Http.HttpRequestException ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
                return View();
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex.Message);
                _logger.LogCritical(ex.StackTrace);
                return View();
            }
        }

        // GET: Career/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: Career/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                // TODO: Add delete logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}