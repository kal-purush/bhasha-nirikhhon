using System;
using System.Collections.Generic;

namespace Web.Model
{
    public abstract class ClassificationScheme<TId, TClassificationScheme, TClassificationSchemeClass, TClass>: DomainObject<TId>
        where TClassificationScheme : ClassificationScheme<TId, TClassificationScheme, TClassificationSchemeClass, TClass>
        where TClassificationSchemeClass : ClassificationSchemeClass<TId, TClassificationScheme, TClassificationSchemeClass, TClass>
    {
        public IList<TClassificationSchemeClass> Classes { get; set; }

        public ClassificationScheme() : base()
        {
        }
    }

    public abstract class ClassificationSchemeClass<TId, TClassificationScheme, TClassificationSchemeClass, TClass>:
        DomainObject<TId>
        where TClassificationScheme : ClassificationScheme<TId, TClassificationScheme, TClassificationSchemeClass, TClass>
        where TClassificationSchemeClass : ClassificationSchemeClass<TId, TClassificationScheme, TClassificationSchemeClass, TClass>
    {
        public TClass                                    Class    { get; set; }
        public TClassificationSchemeClass                Super    { get; set; }
        public virtual IList<TClassificationSchemeClass> Sub      { get; set; }
        public Range<int>                                Interval { get; set; }

        public ClassificationSchemeClass() : base()
        {
        }
    }

    public class Class: Named<Guid>
    {
        public Class() : base()
        {
        }
    }

    public class ClassificationScheme: ClassificationScheme<Guid, ClassificationScheme, ClassificationSchemeClass, Class>
    {
        public ClassificationScheme() : base()
        {
        }
    }

    public class ClassificationSchemeClass: ClassificationSchemeClass<Guid, ClassificationScheme, ClassificationSchemeClass, Class>
    {
        public ClassificationSchemeClass() : base()
        {
        }
    }
}