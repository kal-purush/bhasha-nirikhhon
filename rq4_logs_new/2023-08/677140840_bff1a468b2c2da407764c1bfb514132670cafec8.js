const extensionsToDisable = [
  "EXT_blend_minmax",
  "EXT_disjoint_timer_query",
  "EXT_shader_texture_lod",
  "EXT_sRGB",
  "OES_vertex_array_object",
  "WEBGL_compressed_texture_s3tc_srgb",
  "WEBGL_debug_shaders",
  "WEBKIT_WEBGL_depth_texture",
  "WEBGL_draw_buffers",
  "WEBGL_lose_context",
  "WEBKIT_WEBGL_lose_context",
];

WebGLRenderingContext.prototype.getExtension = (function (oldFn) {
  return function (extensionName) {
    if (extensionsToDisable.indexOf(extensionName) >= 0) {
      return null;
    }
    return oldFn.call(this, name);
  };
})(WebGLRenderingContext.prototype.getExtension);

WebGLRenderingContext.prototype.getSupportedExtensions = (function (oldFn) {
  return function () {
    const extensions = oldFn.call(this);
    return extensions.filter((e) => extensionsToDisable.indexOf(e) < 0);
  };
})(WebGLRenderingContext.prototype.getSupportedExtensions);

const gl = document.createElement("canvas").getContext("webgl");
console.log(gl.getSupportedExtensions().join("\n"));
console.log("WEBGL_draw_buffers:", gl.getExtension("WEBGL_draw_buffers"));