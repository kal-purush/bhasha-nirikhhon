interface SerializeJavascriptStatic {
  (obj: any): any;
}

declare module 'serialize-javascript' {
  var serialize_javascript: SerializeJavascriptStatic;
  export default serialize_javascript;
}