/* All images should be declared here so that they start loading as soon as the application starts
 */



function makeImage(src: string): HTMLImageElement {
    const img = new Image();
    img.src = src;
    return img;
}

// UI images
export const HighlightSelectionImage = makeImage("assets/ui/highlight.png");

// tile textures
export const HabitatTexture = makeImage("assets/tiles/habitat.png");
export const WastelandTexture = makeImage("assets/tiles/wasteland.png");
export const SolarPanelsTexture = makeImage("assets/tiles/solar_panels.png");
export const MountainTexture = makeImage("assets/tiles/mountain.png");
export const MiningFacilityTexture = makeImage("assets/tiles/mining_facility.png");
export const LanderTexture = makeImage("assets/tiles/lander.png");