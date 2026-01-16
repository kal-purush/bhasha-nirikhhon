namespace System.Reflection
{
    public static class ObjectExtensions
    {
        public static void SetValue(this object obj, string propertyName, object value, params object[] indexer)
        {
            obj.GetType().GetAnyProperty(propertyName).SetValue(obj, value, indexer);
        }

        public static T GetValue<T>(this object obj, string propertyName, params object[] indexer)
        {
            return (T)obj.GetType().GetAnyProperty(propertyName).GetValue(obj, indexer as object[]);
        }
    }
}