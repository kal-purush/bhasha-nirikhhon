
var kinds: {
    [name: string]: Pluglets.Kind<any>;
} = {};

export function kind(name: string) {
    return kinds[name] || (kinds[name] = {});
}