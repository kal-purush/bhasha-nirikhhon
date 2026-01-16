using Disney.Domain.DTOs;
using Disney.Domain.Entities;
using Disney.Infrastructure.Interfaces;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Disney.Infrastructure.Repositories
{
    public class CharactersRepository : IBaseRepository<Character>
    {
        #region DataContext and Constructor
        private readonly DataContext context;
        public CharactersRepository(DataContext context)
            => this.context = context;
        #endregion

        public async Task<IEnumerable<Character>> GetAll()
        {
            var result = await context.Characters
                .Where(x => x.Status == true)
                .OrderBy(x => x.Name)
                .ToListAsync();

            return (IQueryable<Character>)result;
        }
        public async Task<Character> GetbyId(int id)
        {
            var result = context.Characters
                .Where(x => x.ID == id)
                .FirstOrDefault();

            return result;
        }
        public IQueryable<Character> GetbyAge(int age)
        {
            var result = context.Characters
                .Where(x => x.Status == true && x.Age == age)
                .ToList();

            return (IQueryable<Character>)result;
        }
        public Character GetbyName(string name)
        {
            var result = context.Characters
                .Where(x => x.Name.Contains(name) && x.Status == true)
                .FirstOrDefault();

            return result;
        }
        public async Task<IQueryable<Character>> GetbyMovieSerie(MovieSerieDTO movieSerieDTO)
        {
            var result = await context.MoviesSeries
                .Where(x => x.Name == movieSerieDTO.Name)
                .Include(x => x.associatedCharacter)
                             .OrderBy(x => x.associatedCharacter.Name)
                             .ToListAsync();

            return (IQueryable<Character>)result;
        }
        public async Task Create(Character entity)
        {
            await context.Characters.AddAsync(entity);
            await context.SaveChangesAsync();
        }
        public async Task Update(Character entity)
        {
            context.Characters.Update(entity);
            await context.SaveChangesAsync();
        }
        public bool CharacterExists(Character entity)
        {
            bool result = true;

            var search = context.Characters
                .Where(x => x.ID == entity.ID)
                .FirstOrDefault();

            if (search == null)
            {
                result = false;
            }

            return result;
        }
    }
}