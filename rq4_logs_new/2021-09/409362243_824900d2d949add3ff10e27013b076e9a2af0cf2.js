export let assetsToLoad = [];
export let loadedAssets = 0;

export function addToLoad(img) {
    assetsToLoad.push(img);
}
export function loadEvent() {
    assetsToLoad.map( a => {
        a.onload = () => {
            loadedAssets++;
        };
    });
}