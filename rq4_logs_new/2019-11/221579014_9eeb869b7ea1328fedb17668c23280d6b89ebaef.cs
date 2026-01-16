using ChajdPizzaWebApp.Models;
using ChajdPizzaWebApp.Repositories.Interfaces;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChajdPizzaWebApp.Data
{
    public class StateRepo : IStateRepo
    {
        readonly ApplicationDbContext _context;

        public StateRepo()
        {

        }

        public StateRepo(ApplicationDbContext ctx)
        {
            _context = ctx;
        }

        public async Task<IEnumerable<State>> GetStates()
        {
            IEnumerable<State> result = null;

            var query = _context.States.Where(s => s.ID == s.ID);
            if (query.Count() > 0)
            {
                result = await query.ToListAsync();
            }
            else
            {
                throw new NullReferenceException("EMPTY QUERY IN STATE REPO!");
            }

            return result;
        }

        public async Task<State> GetState(int id)
        {
            State result = null;

            var query = _context.States.Where(s => s.ID == id);
            if (query.Count() > 0)
            {
                result = await query.FirstOrDefaultAsync();
            }
            else
            {
                throw new NullReferenceException("EMPTY QUERY IN STATE REPO!");
            }

            return result;
        }

        public async Task<string> GetStateName(int id)
        {
            State temp = null;
            string result = null;

            var query = _context.States.Where(s => s.ID == id);
            if (query.Count() > 0)
            {
                temp = await query.FirstOrDefaultAsync();
                result = temp.Name;
            }
            else
            {
                throw new NullReferenceException("EMPTY QUERY IN STATE REPO!");
            }

            return result;
        }

        public async Task<string> GetStateAbbrevation(int id)
        {
            State temp = null;
            string result = null;

            var query = _context.States.Where(s => s.ID == id);
            if (query.Count() > 0)
            {
                temp = await query.FirstOrDefaultAsync();
                result = temp.Abbreviation;
            }
            else
            {
                throw new NullReferenceException("EMPTY QUERY IN STATE REPO!");
            }

            return result;
        }
    }
}