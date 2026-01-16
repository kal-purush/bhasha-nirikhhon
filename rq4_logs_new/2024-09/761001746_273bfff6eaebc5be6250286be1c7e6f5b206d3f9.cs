using Microsoft.AspNetCore.Components;

namespace BlazorAddons.Internal
{
    /// <summary>
    /// Assists with rendering <see cref="Overlay"/> remotely.
    /// </summary>
    public class RemoteItem
    {
        /// <summary>
        /// What gets rendered.
        /// </summary>
        public RenderFragment Renderer;
        /// <summary>
        /// The handler for this item.
        /// </summary>
        public RemoteRendererSingle? RenderOwner;
        /// <summary>
        /// An offset to the z-index of the overlay.
        /// </summary>
        public int ZIndexOffset;

        /// <summary>
        /// Creates a new <see cref="RemoteItem"/>.
        /// </summary>
        public RemoteItem(RenderFragment renderer, int zIndexOffset = 0)
        {
            Renderer = renderer;
            ZIndexOffset = zIndexOffset;
        }
    }
}