module JsonHelper {
    export function toDebugJson(src:any,...ignoreFields: string[]):string {
        var items = _.pairs(src);
        var obj = new Object();
        _(items).filter(i=> i[0].indexOf("$") != 0) //フレームワークのオブジェクトを除外
            .filter(i=> ignoreFields.indexOf(i[0]) == -1) //指定した名前を除外しておく
            .filter(i=> !_.isFunction(i[1]))//関数を除外
            .filter(i=> i[1] != src)　　　 //自身のオブジェクトを除外(無限ループ対策)
            .forEach(i=> obj[i[0]] = JsonHelper.objectFilter(i[1]));

        var str = JSON.stringify(obj, null, "    ");
        return str;
    }

    export function objectFilter(src: any):any {
        var items = _.pairs(src);

        if (items.length == 0)
            return src;

        var dst = new Object();
        _(items).filter(i=> i[0].indexOf("$") != 0) //フレームワークのオブジェクトを除外
            .filter(i=> !_.isFunction(i[1]))//関数を除外
            .filter(i=> i[1] != src)　　　 //自身のオブジェクトを除外(無限ループ対策)
            .forEach(i=> dst[i[0]] = JsonHelper.objectFilter(i[1]));
        return dst;
    }
} 