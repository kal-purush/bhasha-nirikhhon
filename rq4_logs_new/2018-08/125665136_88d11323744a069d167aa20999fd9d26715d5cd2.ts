import { RdAngularCompilerModule, CompiledResultModel, TemplateCompiler, Compiler } from '@rd/compiler';

require("rxjs")
var ngCore = require("@angular/core")
require("@angular/common")
require("@angular/forms")
require("@angular/platform-browser")
require("@angular/http")
require("@angular/platform-browser-dynamic")
require("@angular/compiler")
// var compiler = require("@rd/compiler")
var ngCore = require("@angular/core")
var compiler: {
    RdAngularCompilerModule: RdAngularCompilerModule,
    CompiledResultModel: CompiledResultModel,
    Compiler: Compiler,
    TemplateCompiler: TemplateCompiler
} = require("@angular/compiler")
// var jitCompiler = new compiler.JitCompiler()
// jitCompiler.compileModuleAndAllComponentsAsync()
// compiler.CompiledResultModel()