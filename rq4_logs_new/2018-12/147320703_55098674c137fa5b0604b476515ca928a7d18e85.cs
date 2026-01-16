
namespace BonVoyage
{
    /// <summary>
    /// Information about batteries
    /// </summary>
    internal class Batteries
    {
        internal bool UseBatteries; // Use batteries during a night
        internal double MaxAvailableEC; // Max EC available from all activated batteries
        internal double MaxUsedEC; // Max EC we can use
        internal double ECPerSecondConsumed; // EC per second consumed by wheels
        internal double ECPerSecondGenerated; // EC per second generated (generated power minus required power)
        internal double CurrentEC; // Current EC status of batteries
    }

}