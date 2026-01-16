namespace Newtonsoft.Helper
{
    #region Usings
    using System;
    using System.Collections;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json;
    using System.Collections.Generic;
    using System.Linq;
    using NLog;
    #endregion

    public static class JObjectExtensions
    {
        #region Logger
        private static readonly Logger logger = LogManager.GetCurrentClassLogger();
        #endregion

        #region Public Methods
        public static JArray MergeArray(this JObject @this, JArray array, List<string> keyNames)
        {
            try
            {
                foreach (var keyName in keyNames)
                {
                    var foundTokens = array.SelectTokens($"$..name")?.Where(t => t.Value<string>() == keyName)?.ToList();
                    if (foundTokens.Count > 1)
                    {
                        var firstToken = foundTokens.FirstOrDefault()?.Parent?.Parent?.ToObject<JObject>() ?? null;
                        for (int i = foundTokens.Count - 1; i >= 1; i--)
                        {
                            var rawToken = foundTokens?.ElementAtOrDefault(i) ?? null;
                            var token = rawToken.Parent?.Parent?.ToObject<JObject>() ?? null;
                            firstToken?.Merge(token);
                            rawToken?.Parent?.Parent?.Remove();
                        }
                        foundTokens[0]?.Parent?.Parent?.Replace(firstToken);
                    }
                }
                return array;
            }
            catch (Exception ex)
            {
                logger.Error(ex, "Array could not merge.");
                return null;
            }
        }
        #endregion
    }
}