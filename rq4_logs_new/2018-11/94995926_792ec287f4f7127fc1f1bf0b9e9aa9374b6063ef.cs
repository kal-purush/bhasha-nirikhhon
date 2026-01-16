namespace Lykke.Common.ApiLibrary.Contract
{
    /// <summary>
    ///     Error code for any expected errors.
    /// </summary>
    public interface ILykkeApiErrorCode
    {
        /// <summary>
        ///     Error code name that would be returned as contract.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Default message describing error code reason.
        /// </summary>
        string DefaultMessage { get; }
    }
}