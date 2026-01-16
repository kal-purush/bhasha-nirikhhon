using System;

namespace GetcuReone.Cdi
{
    /// <inheritdoc />
    [Obsolete("[3.0.2] Use BaseGrAdapterProxy")]
    public abstract class GrAdapterProxyBase<TProxy, TProxyParam> : BaseGrAdapterProxy<TProxy, TProxyParam>
    {
        /// <inheritdoc />
        protected GrAdapterProxyBase(Func<TProxyParam, TProxy> createProxyFunc) : base(createProxyFunc) { }
    }
}