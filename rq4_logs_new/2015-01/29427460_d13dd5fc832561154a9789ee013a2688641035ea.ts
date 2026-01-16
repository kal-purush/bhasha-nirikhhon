module hh{

    /**
     * 拼接字符串成路径。
     * @param args
     * @returns {string}
     */
    export function joinPath(...args:string[]):string{
        var l = args.length;
        var result = "";
        for(var i = 0; i < l; i++) {
            result = (result + (result == "" ? "" : "/") + args[i]);
        }
        result = result.replace(/\\\\$/, "");
        result = result.replace(/\\$/, "");
        result = result.replace(/[\/]+$/, "/");
        return result;
    }

    /**
     * 获取文件后缀名，主要，后缀名带"."，例如：".png"。
     * @param pathStr
     * @returns {string}
     */
    export function extName(pathStr:string):string{
        var temp = /(\.[^\.\/\?\\]*)(\?.*)?$/.exec(pathStr);
        return temp ? temp[1] : null;
    }

    /**
     * 获取文件名，如果传了extname参数，那么就将去除后缀名。
     * 注意，跟nodejs不同的是，extname参数不区分大小写
     * @param pathStr
     * @param extname
     * @returns {string}
     */
    export function baseName(pathStr:string, extname?:string):string{
        var index = pathStr.indexOf("?");
        if(index > 0) pathStr = pathStr.substring(0, index);
        var baseName:string;
        if(!pathStr.match(/(\/|\\\\)/)) {
            baseName = pathStr;
        }else{
            var reg = /(\/|\\\\)([^(\/|\\\\)]+)$/g;
            var result = reg.exec(pathStr.replace(/(\/|\\\\)$/, ""));
            if(!result) return null;
            baseName = result[2];
        }
        if(extname && pathStr.substring(pathStr.length - extname.length).toLowerCase() == extname.toLowerCase())
            return baseName.substring(0, baseName.length - extname.length);
        return baseName;
    }

    /**
     * 获取文件所在目录路径。
     * @param pathStr
     * @returns {string}
     */
    export function dirName(pathStr:string):string{
        var dirname = pathStr.replace(/(\/|\\\\)$/, "").replace(/(\/|\\\\)[^(\/|\\\\)]+$/, "");
        if(dirname == pathStr) return "";
        return dirname;
    }

    export function relative(rootPath:string, realPath:string){
        rootPath = rootPath.replace(/\\/g, "/");
        rootPath = rootPath.replace(/[\/]+/g, "/");

        realPath = realPath.replace(/\\/g, "/");
        realPath = realPath.replace(/[\/]+/g, "/");

        if(rootPath.lastIndexOf("/") != rootPath.length - 1) rootPath += "/";
        if(rootPath == realPath || rootPath.substring(0, rootPath.length - 1) == realPath) return "./";
        if(realPath.indexOf(rootPath) != 0){
            console.error("路径【%s】无法到【%s】进行相对路径处理，请检查！", realPath, rootPath);
            return null;
        }
        return realPath.substring(rootPath.length);
    }

    //TODO 还有写api没有写

    //------------这是path模块的拓展--------
    /**
     * 替换一个文件的文件后缀名。
     * @param pathStr
     * @param extname
     * @returns {string}
     */
    export function changeExtName(pathStr:string, extname?:string):string{
        extname = extname || "";
        var index:number = pathStr.indexOf("?");
        var tempStr:string = "";
        if(index > 0) {
            tempStr = pathStr.substring(index);
            pathStr = pathStr.substring(0, index);
        }
        index = pathStr.lastIndexOf(".");
        if(index < 0) return pathStr + extname + tempStr;
        return pathStr.substring(0, index) + extname + tempStr;
    }

    /**
     * 改变文件的basename。
     * @param pathStr
     * @param basename
     * @param isSameExt
     * @returns {string}
     */
    export function changeBaseName(pathStr:string, basename:string, isSameExt?:boolean){
        if(basename.indexOf(".") == 0) return this.changeExtname(pathStr, basename);//如果basename就是一个extname
        var index:number = pathStr.indexOf("?");
        var tempStr:string = "";
        var ext = isSameExt ? this.extname(pathStr) : "";
        if(index > 0) {
            tempStr = pathStr.substring(index);
            pathStr = pathStr.substring(0, index);
        }
        index = pathStr.lastIndexOf("/");
        index = index <= 0 ? 0 : index+1;
        return pathStr.substring(0, index) + basename + ext + tempStr;
    }
}