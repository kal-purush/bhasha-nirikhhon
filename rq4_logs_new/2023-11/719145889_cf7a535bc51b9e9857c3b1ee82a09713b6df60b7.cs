using legallead.json.db.interfaces;
using legallead.json.entities;
using legallead.json.interfaces;
using System.Reflection;

namespace legallead.json.db
{
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance",
        "CA1822:Mark members as static", Justification = "Exposing generic static methods as public to follow DI pattern")]

    public class DataProvider : IDataProvider
    {
        private const string dbFolder = "_db";
        private const string typeAssignMessage = "improper assignment of typename argument.";
        private static readonly object _instance = new();

        private static readonly Dictionary<string, string> _dataFiles = new();
        public DataProvider()
        {
            Initialize();
        }

        public T Insert<T>(T entity) where T : class, IDataEntity, new()
        {
            if (entity is not BaseEntity<T> baseDto)
                throw new TypeAccessException(typeAssignMessage);

            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];
            baseDto.Add(entity, data);
            return entity;
        }

        public T Update<T>(T entity) where T : BaseEntity<T>, IDataEntity, new()
        {
            if (entity is not BaseEntity<T> baseDto)
                throw new TypeAccessException(typeAssignMessage);

            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();

            var data = _dataFiles[key];
            baseDto.Save(entity, data);
            return entity;
        }

        public T Delete<T>(T entity) where T : BaseEntity<T>, IDataEntity, new()
        {
            if (entity is not BaseEntity<T> baseDto)
                throw new TypeAccessException(typeAssignMessage);

            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];

            baseDto.Remove(entity, data);

            return entity;
        }

        public T? FirstOrDefault<T>(Func<T, bool> expression) where T : BaseEntity<T>, IDataEntity, new()
        {
            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];

            return BaseEntity<T>.Get(data, expression);
        }
        public T? FirstOrDefault<T>(T entity, Func<T, bool> expression) where T : BaseEntity<T>, IDataEntity, new()
        {
            if (entity is not BaseEntity<T> baseDto)
                throw new TypeAccessException(typeAssignMessage);

            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];

            return BaseEntity<T>.Get(data, expression);
        }

        public IEnumerable<T>? Where<T>(Func<T, bool> expression) where T : BaseEntity<T>, IDataEntity, new()
        {
            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];

            return BaseEntity<T>.GetAll(data, expression);
        }

        public IEnumerable<T>? Where<T>(T entity, Func<T, bool> expression)
            where T : BaseEntity<T>, IDataEntity, new()
        {
            if (entity is not BaseEntity<T> baseDto)
                throw new TypeAccessException(typeAssignMessage);

            var key = typeof(T).Name.ToLower();
            if (!_dataFiles.ContainsKey(key))
                throw new FileNotFoundException();
            var data = _dataFiles[key];

            return BaseEntity<T>.GetAll(data, expression);
        }


        private void Generate(string entityName, ref string? location)
        {
            var assembly = Assembly.GetExecutingAssembly();
            if (assembly == null || assembly.Location == null) return;
            var execName = new Uri(assembly.Location).AbsolutePath;
            if (execName != null && File.Exists(execName))
            {
                var name = $"{entityName}.json";
                var contentRoot = Path.GetDirectoryName(execName) ?? "";
                var dataRoot = Path.Combine(contentRoot, dbFolder);
                if (!Directory.Exists(dataRoot)) { Directory.CreateDirectory(dataRoot); }
                var dataFile = Path.Combine(dataRoot, name);
                if (File.Exists(dataFile))
                {
                    location = dataFile;
                    return;
                }
                lock (_instance)
                {
                    var content = "[]";
                    File.WriteAllText(dataFile, content);
                }
                location = dataFile;
            }
        }

        private void Initialize()
        {
            var type = typeof(IDataEntity);
            var types = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(s => s.GetTypes())
                .Where(p => type.IsAssignableFrom(p) && !p.IsAbstract && !p.IsInterface)
                .Select(s => new { Name = s.Name.ToLowerInvariant(), type = s })
                .ToList();
            types.ForEach(t =>
            {
                var typeName = t.Name.ToLowerInvariant();
                var location = GetFileLocation(typeName);
                Generate(typeName, ref location);
                location ??= string.Empty;
                if (_dataFiles.ContainsKey(t.Name))
                {
                    _dataFiles.Remove(t.Name);
                }
                _dataFiles.Add(t.Name, location);
            });
        }

        private string GetFileLocation(string typeName)
        {
            var assembly = Assembly.GetExecutingAssembly();
            if (assembly == null || assembly.Location == null) return string.Empty;
            var execName = new Uri(assembly.Location).AbsolutePath;
            if (execName != null && File.Exists(execName))
            {

                var name = $"{typeName}.json";
                var contentRoot = Path.GetDirectoryName(execName) ?? "";
                var dataRoot = Path.Combine(contentRoot, dbFolder);
                if (!Directory.Exists(dataRoot)) { Directory.CreateDirectory(dataRoot); }
                var dataFile = Path.Combine(dataRoot, name);
                if (!File.Exists(dataFile))
                {
                    lock (_instance)
                    {
                        var content = "[]";
                        File.WriteAllText(dataFile, content);
                    }
                }
                return dataFile;
            }
            return string.Empty;
        }
    }
}