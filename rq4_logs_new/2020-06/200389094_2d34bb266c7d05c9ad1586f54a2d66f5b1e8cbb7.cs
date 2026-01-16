using CommonDomainObjects;
using System;

namespace Deals
{
    public class DealClassifier: DomainObject
    {
        public Deal       Deal       { get; protected set; }
        public Classifier Classifier { get; protected set; }

        protected DealClassifier() : base()
        {
        }

        public DealClassifier(
            Deal       deal,
            Classifier classifier
            ) : base()
        {
            Deal       = deal;
            Classifier = classifier;
        }

        public override bool Equals(
            object obj
            ) => obj is DealClassifier dealClassifier &&
                Deal       == dealClassifier.Deal &&
                Classifier == dealClassifier.Classifier;

        public override int GetHashCode()
        {
            return HashCode.Combine(
                Deal,
                Classifier);
        }

        public static bool operator ==(
            DealClassifier lhs,
            DealClassifier rhs
            ) => Equals(
                lhs,
                rhs);

        public static bool operator !=(
            DealClassifier lhs,
            DealClassifier rhs
            ) => !(lhs == rhs);
    }
}