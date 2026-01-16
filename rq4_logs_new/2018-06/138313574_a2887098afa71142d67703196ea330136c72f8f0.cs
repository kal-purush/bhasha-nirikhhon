using dal.apifinport.Context;
using entities.apifinport.Entities;
using entities.apifinport.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System;
using System.Linq;

namespace apicore.Controllers
{
    public class BaseController<T> : ControllerBase where T : class, IEntity
    {
        private readonly FinPortContext _context;

        public BaseController(FinPortContext context)
        {
            _context = context;
        }

        // [HttpGet]
        // public virtual ActionResult<JResponseEntity<T>> List()
        // {
        //     JResponseEntity<T> JsonResponse = new JResponseEntity<T>();
        //     try
        //     {
        //         var data = _context.Set<T>().ToList();
        //         JsonResponse.Status = true;
        //         JsonResponse.DataList = data;
        //         JsonResponse.Message = "Success on request.";
        //     }
        //     catch (Exception e)
        //     {
        //         JsonResponse.Message = e.InnerException != null ? e.InnerException.Message : e.Message;
        //     }
        //     return JsonResponse;
        // }

        [HttpPost]
        public virtual JResponseEntity<T> Create(T entity)
        {
            if (ModelState.IsValid)
            {
                _context.Set<T>().Add(entity);
                _context.SaveChanges();
            }
            return new JResponseEntity<T>();
        }

        [HttpPost]
        public virtual JResponseEntity<T> Edit(T entity)
        {
            if (ModelState.IsValid)
            {
                _context.Entry<T>(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
            return new JResponseEntity<T>();
        }

        [HttpPost]
        public virtual JResponseEntity<T> DeleteEntity(int id)
        {
            if (ModelState.IsValid)
            {
                var entity = _context.Set<T>().Where(x => x.Id == id).FirstOrDefault();
                entity.Deleted = true;
                _context.Entry<T>(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
            return new JResponseEntity<T>();
        }

        [HttpPost]
        public virtual JResponseEntity<T> RecoverEntity(int id)
        {
            if (ModelState.IsValid)
            {
                var entity = _context.Set<T>().Where(x => x.Id == id).FirstOrDefault();
                entity.Deleted = false;
                _context.Entry<T>(entity).State = EntityState.Modified;
                _context.SaveChanges();
            }
            return new JResponseEntity<T>();
        }
    }
}











