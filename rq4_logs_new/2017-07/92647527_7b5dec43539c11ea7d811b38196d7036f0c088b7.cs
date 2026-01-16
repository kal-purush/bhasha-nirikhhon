using System;

namespace MAF.FeaturesFlipping
{
    public struct FeatureName
    {
        public FeatureName(string application, string scope, string feature)
        {
            Application = application ?? string.Empty;
            Scope = scope ?? string.Empty;
            Feature = feature ?? string.Empty;
        }
        public string Application { get; }
        public string Scope { get; }
        public string Feature { get; }

        public override int GetHashCode()
        {
            unchecked
            {
                int hash = 17;
                hash = hash * 23 + Application.GetHashCode();
                hash = hash * 23 + Scope.GetHashCode();
                hash = hash * 23 + Feature.GetHashCode();
                return hash;
            }
        }

        public override bool Equals(object obj)
        {
            return obj is FeatureName featureName
                   && Application.Equals(featureName.Application, StringComparison.CurrentCulture)
                   && Scope.Equals(featureName.Scope, StringComparison.CurrentCulture)
                   && Feature.Equals(featureName.Feature, StringComparison.CurrentCulture);
        }

        public override string ToString()
        {
            return $"<Application={Application};Scope={Scope};Feature={Feature}>";
        }
    }
}