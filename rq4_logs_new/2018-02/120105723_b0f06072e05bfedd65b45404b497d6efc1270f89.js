{
    const href = window.location.href;      
    if (href.indexOf("/components/hb-app") > -1) {
        window.Polymer = {
        rootPath: href.substring(href.indexOf("/components/hb-app"), href.lastIndexOf("/")) 
        };
    }
}