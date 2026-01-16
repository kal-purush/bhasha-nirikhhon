using System.Linq.Expressions;
using MedicalExaminationPreliminaryLists.Data;
using MedicalExaminationPreliminaryLists.Data.Common;
using Microsoft.EntityFrameworkCore;

namespace MedicalExaminationPreliminaryLists.Infrastructure.Common
{
    public abstract class GenericGUIDRepository<T> : IGUIDRepository<T>, IDisposable where T : BaseGUIDEntity
    {
        protected readonly AppDbContext _contextFactory;
        private bool _disposedValue;

        protected GenericGUIDRepository(AppDbContext contextFactory)
        {
            _contextFactory = contextFactory;
        }

        //Получение всех актуальных (неудаленных) данных IQueryable GetAll();
        public virtual IQueryable<T> GetAll()
        {
            IQueryable<T> query = _contextFactory.Set<T>().AsNoTracking().Where(w => !w.IsDeleted);
            return query;
        }

        //Получение всех данных, в том числе и помеченных на удаление IQueryable GetAllWithDeleted();
        public virtual IQueryable<T> GetAllWithDeleted()
        {
            IQueryable<T> query = _contextFactory.Set<T>();
            return query;
        }

        //Получение экземпляра по ключу (асинхронно) Task<T?> GetByKeyAsync(int id);
        public async Task<T?> GetByKeyAsync(Guid id)
        {
            return await _contextFactory.Set<T>().FindAsync(id);
        }

        //Получение экземпляра по ключу(синхронно)T? GetByKey(int id);
        public virtual T? GetByKey(Guid id)
        {
            var query = _contextFactory.Set<T>().Find(id);
            return query;
        }
        //Получение актуальной выборки по условию IQueryable FindBy(Expression<Func<T, bool>> predicate);
        public virtual IQueryable<T> FindBy(Expression<Func<T, bool>> predicate)
        {
            IQueryable<T> query = _contextFactory.Set<T>().AsNoTracking().Where(w => !w.IsDeleted).Where(predicate);
            return query;
        }
        //Получение полной выборки(в т.ч.удаленных) по условию Task<T?> FirstAsync(Expression<Func<T, bool>> predicate);
        public async Task<T?> FirstAsync(Expression<Func<T, bool>> predicate)
        {
            return await _contextFactory.Set<T>().FirstOrDefaultAsync(predicate);
        }
        //Получение первого экземпляра по условию T? First(Expression<Func<T, bool>> predicate);
        public virtual T? First(Expression<Func<T, bool>> predicate)
        {
            return _contextFactory.Set<T>().FirstOrDefault(predicate);
        }
        //Добавление данных Add(T entity);
        public virtual void Add(T entity)
        {
            // UtcNOW ибо Постгре ругается, ему нужен формат UTC
            entity.CreateDate = DateTime.UtcNow;
            entity.EditDate = DateTime.UtcNow;
            _contextFactory.Set<T>().Add(entity);
        }
        //Удаление из БД Delete(T entity);
        public virtual void Delete(T entity)
        {
            _contextFactory.Set<T>().Remove(entity);
        }
        //Пометка на удаление по объекту
        public virtual async Task VirtualDelete(T entity, int userId)
        {
            entity.IsDeleted = true;
            entity.DeleteDate = DateTime.Now;
            await SaveChangesAsync();
        }
        //Пометка на удаление по ключу объекта
        public virtual async Task VirtualDelete(Guid Id, int userId)
        {
            T? entity = _contextFactory.Set<T>().FirstOrDefault(w => w.Id == Id);
            if (entity != null)
            {
                entity.IsDeleted = true;
                entity.DeleteDate = DateTime.UtcNow; // UtcNow - для
                await SaveChangesAsync();
            }
        }
        //Изменение данных void Edit(T entity);
        public virtual void Edit(T entity)
        {
            var entry = _contextFactory.Entry(entity);
            if (entry.State != EntityState.Added)
                entry.State = EntityState.Modified;
        }
        //Сохранение изменений(синхронно) void Save();
        public virtual void Save()
        {
            _contextFactory.SaveChanges();
        }

        //Сохранение изменений(асинхронно) Task SaveChangesAsync();
        public async Task SaveChangesAsync()
        {
            await _contextFactory.SaveChangesAsync();
        }
        //Поиск с пропуском
        public IQueryable<T> FindByWithTake(Expression<Func<T, bool>> predicate, int skip, int take)
        {
            IQueryable<T> query = _contextFactory.Set<T>().AsNoTracking().Where(w => !w.IsDeleted).Where(predicate).Skip(skip).Take(take);
            return query;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _contextFactory.Dispose();
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}