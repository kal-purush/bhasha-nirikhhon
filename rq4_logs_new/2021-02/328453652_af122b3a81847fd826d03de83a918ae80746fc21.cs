// <copyright file="FileDbContext.cs" company="PlaceholderCompany">
// Copyright (c) PlaceholderCompany. All rights reserved.
// </copyright>

namespace EducationPortal.DAL.DbContexts
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using EducationPortal.DAL.Entities;
    using EducationPortal.DAL.Utilities;

    public class FileDbContext
    {
        private IDictionary<TableNames, DbTable> tables;
        private JsonFileHelper fileHelper;
        private DbWatcher fileWatcher;

        public FileDbContext()
        {
            this.tables = new Dictionary<TableNames, DbTable>();
            this.fileHelper = JsonFileHelper.GetInstance();
            this.fileWatcher = DbWatcher.GetInstance();

            this.fileWatcher[TableNames.User].Created += this.OnCreate<User>;
            this.fileWatcher[TableNames.User].Changed += this.OnChange<User>;
            this.fileWatcher[TableNames.User].Deleted += this.OnDelete<User>;

            // this.fileWatcher[TableNames.Account].Created += this.OnCreate<Account>;

            // this.fileWatcher[TableNames.Account].Changed += this.OnChange<Account>;

            // this.fileWatcher[TableNames.Account].Deleted += this.OnDelete<Account>;
            this.fileWatcher[TableNames.Skill].Created += this.OnCreate<Material>;
            this.fileWatcher[TableNames.Skill].Changed += this.OnChange<Material>;
            this.fileWatcher[TableNames.Skill].Deleted += this.OnDelete<Material>;
        }

        public IDbTable GetTable<T>()
            where T : Entity
        {
            var tableNameStr = typeof(T).Name;
            var tableName = (TableNames)Enum.Parse(typeof(TableNames), tableNameStr);

            if (!this.tables.ContainsKey(tableName))
            {
                var tablePath = DbConfig.DbPathPrefix + DbConfig.TablePaths[tableName];
                IEnumerable<T> tableContent = this.fileHelper.ReadTable<T>(tablePath);
                var table = new DbTable(tableContent);
                this.tables.Add(tableName, table);
            }

            return this.tables[tableName];
        }

        public void Save<T>()
        {
            var tableNameStr = typeof(T).Name;
            var tableName = (TableNames)Enum.Parse(typeof(TableNames), tableNameStr);

            var entitiesToSave = new Dictionary<Entity, EntityState>();

            var currentTable = this.tables[tableName];
            var tablePath = DbConfig.DbPathPrefix + DbConfig.TablePaths[tableName];

            foreach (var entity in currentTable.ContentWithState)
            {
                if (entity.Value != EntityState.Synchronized)
                {
                    entitiesToSave.Add(entity.Key, entity.Value);
                }
            }

            this.fileWatcher[tableName].EnableRaisingEvents = false;
            this.fileHelper.SaveTable(tablePath, entitiesToSave);
            this.fileWatcher[tableName].EnableRaisingEvents = true;

            foreach (var entity in entitiesToSave.Keys)
            {
                if (currentTable.GetEntityState(entity) == EntityState.Deleted)
                {
                    currentTable.RemoveFromTable(entity);
                }
                else
                {
                    currentTable.SetEntityState(entity, EntityState.Synchronized);
                }
            }
        }

        private void OnCreate<T>(object sender, FileSystemEventArgs args)
            where T : Entity
        {
            var tablePath = Path.GetDirectoryName(args.FullPath) + "/";
            var idStr = Path.GetFileNameWithoutExtension(args.FullPath);
            var isParsed = long.TryParse(idStr, out long id);

            if (isParsed)
            {
                var createdEntity = this.fileHelper.ReadEntity<T>(tablePath, id);
                var tableNameStr = typeof(T).Name;
                var tableName = (TableNames)Enum.Parse(typeof(TableNames), tableNameStr);

                if (!this.tables.ContainsKey(tableName))
                {
                    IEnumerable<T> tableContent = this.fileHelper.ReadTable<T>(tablePath);
                    var table = new DbTable(tableContent);
                    this.tables.Add(tableName, table);
                }

                this.tables[tableName].Add(createdEntity);
                this.tables[tableName].SetEntityState(createdEntity, EntityState.Synchronized);
            }
        }

        private void OnChange<T>(object sender, FileSystemEventArgs args)
            where T : Entity
        {
            var tablePath = Path.GetDirectoryName(args.FullPath) + "/";
            var idStr = Path.GetFileNameWithoutExtension(args.FullPath);
            var isParsed = long.TryParse(idStr, out long id);

            if (isParsed)
            {
                var changedEntity = this.fileHelper.ReadEntity<T>(tablePath, id);
                var tableNameStr = typeof(T).Name;
                var tableName = (TableNames)Enum.Parse(typeof(TableNames), tableNameStr);

                if (!this.tables.ContainsKey(tableName))
                {
                    IEnumerable<T> tableContent = this.fileHelper.ReadTable<T>(tablePath);
                    var table = new DbTable(tableContent);
                    this.tables.Add(tableName, table);
                }

                this.tables[tableName].Update(changedEntity);
                this.tables[tableName].SetEntityState(changedEntity, EntityState.Synchronized);
            }
        }

        private void OnDelete<T>(object sender, FileSystemEventArgs args)
            where T : Entity, new()
        {
            var tablePath = Path.GetDirectoryName(args.FullPath) + "/";
            var idStr = Path.GetFileNameWithoutExtension(args.FullPath);
            var isParsed = int.TryParse(idStr, out int id);

            if (isParsed)
            {
                var tableNameStr = typeof(T).Name;
                var tableName = (TableNames)Enum.Parse(typeof(TableNames), tableNameStr);

                if (!this.tables.ContainsKey(tableName))
                {
                    IEnumerable<T> tableContent = this.fileHelper.ReadTable<T>(tablePath);
                    var table = new DbTable(tableContent);
                    this.tables.Add(tableName, table);
                }

                this.tables[tableName].Remove(new T() { Id = id });
            }
        }
    }
}