declare var System: any;

System.import("/index.js").then((m: any) => {
    m.init({})
    return m.get("entry")
}).then((m: any) => m());