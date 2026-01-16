using Core.DataAccess.Databases;
using Core.Entities;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Core.DataAccess.MongoDB
{
    public class MongoDB_RepositoryBase<TEntity, MongoDBContext> : IEntityRepository<TEntity>, IDisposable
        where TEntity : class, IEntity, new()
       where MongoDBContext : class, IMongoDB_Context<TEntity>, new()
    {
        public IMongoCollection<TEntity> _collection { get; set; }
        MongoDBContext mongoDBContext = new MongoDBContext();
        public MongoDB_RepositoryBase()
        {

            _collection = mongoDBContext.GetMongoDBCollection();
        }
        public void Add(TEntity entity)
        {
            _collection.InsertOne(entity);
        }
        public DeleteResult Delete(TEntity entity)
        {
            return _collection.DeleteOne(e => e.Id == entity.Id);
        }
        public DeleteResult DeleteMany(Expression<Func<TEntity, bool>> filter = null)
        {
            return _collection.DeleteMany(filter);
        }
        public ReplaceOneResult Update(TEntity entity)
        {

            return _collection.ReplaceOne(e => e.Id == entity.Id, entity);

        }
        public TEntity Get(Expression<Func<TEntity, bool>> filter = null)
        {
            try
            {
                return filter == null
            ? _collection.Find<TEntity>(document => true).SingleOrDefault()
            : _collection.Find<TEntity>(filter).SingleOrDefault();

            }
            catch (Exception e)
            {

                throw new Exception(e.Message);
            }

        }
        public List<TEntity> GetAll(Expression<Func<TEntity, bool>> filter = null)
        {
            try
            {
                return filter == null
            ? _collection.Find<TEntity>(document => true).Sort("{_id:-1}").ToList()
            : _collection.Find<TEntity>(filter).Sort("{_id:-1}").ToList();

            }
            catch (Exception e)
            {

                throw new Exception(e.Message);
            }
        }
        public List<TEntity> GetAllWithPage(int page, int limit, Expression<Func<TEntity, bool>> filter = null)
        {
            return filter == null ?
                _collection.Find(document => true).Sort("{_id:-1}").Skip(page * limit).Limit(limit).ToList() :
                _collection.Find(filter).Sort("{_id:-1}").Skip(page * limit).Limit(limit).ToList();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}