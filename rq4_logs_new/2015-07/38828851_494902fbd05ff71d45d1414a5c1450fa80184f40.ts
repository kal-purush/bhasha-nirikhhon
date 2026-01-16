export function Inject(data: string){
    return function (target: any) {
        Registry[data] = new target();
    };
}
export let Registry = {};