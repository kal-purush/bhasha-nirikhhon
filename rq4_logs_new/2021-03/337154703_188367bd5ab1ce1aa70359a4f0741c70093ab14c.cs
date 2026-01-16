using Lab1.model;
using System;
using System.Collections.Generic;
using System.Text;

namespace Lab1.dao
{
    public class CachedGroupDao : Dao<CachedGroup>, ICachedGroupDao
    {
        public void Delete(CachedGroup group)
        {
            entities.RemoveAll(it => group.Id == it.Id);
        }

        public void Update(CachedGroup group)
        {
            Delete(group);
            Create(group);
        }

        public List<CachedGroup> GroupsByDayOfWeek(DayOfWeek dayOfWeek)
        {
            return GetAll().FindAll(it => it.Group.LessonsDay == dayOfWeek);
        }

        public List<CachedGroup> GroupsByGameLevel(GameLevel level)
        {
            return GetAll().FindAll(it => it.Group.GameLevel == level);
        }
    }
}