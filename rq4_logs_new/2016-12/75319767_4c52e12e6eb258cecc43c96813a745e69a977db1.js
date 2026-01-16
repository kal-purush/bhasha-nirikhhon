/*!
 * XRegExp v2.0.0
 * (c) 2007-2012 Steven Levithan <http://xregexp.com/>
 * MIT License
 */
var XRegExp;XRegExp=XRegExp||(function(undef){"use strict";var self,addToken,add,features={natives:false,extensibility:false},nativ={exec:RegExp.prototype.exec,test:RegExp.prototype.test,match:String.prototype.match,replace:String.prototype.replace,split:String.prototype.split},fixed={},cache={},tokens=[],defaultScope="default",classScope="class",nativeTokens={"default":/^(?:\\(?:0(?:[0-3][0-7]{0,2}|[4-7][0-7]?)?|[1-9]\d*|x[\dA-Fa-f]{2}|u[\dA-Fa-f]{4}|c[A-Za-z]|[\s\S])|\(\?[:=!]|[?*+]\?|{\d+(?:,\d*)?}\??)/,"class":/^(?:\\(?:[0-3][0-7]{0,2}|[4-7][0-7]?|x[\dA-Fa-f]{2}|u[\dA-Fa-f]{4}|c[A-Za-z]|[\s\S]))/},replacementToken=/\$(?:{([\w$]+)}|(\d\d?|[\s\S]))/g,duplicateFlags=/([\s\S])(?=[\s\S]*\1)/g,quantifier=/^(?:[?*+]|{\d+(?:,\d*)?})\??/,compliantExecNpcg=nativ.exec.call(/()??/,"")[1]===undef,hasNativeY=RegExp.prototype.sticky!==undef,isInsideConstructor=false,registeredFlags="gim"+(hasNativeY?"y":"");function augment(regex,captureNames,isNative){var p;for(p in self.prototype){if(self.prototype.hasOwnProperty(p)){regex[p]=self.prototype[p];}}
regex.xregexp={captureNames:captureNames,isNative:!!isNative};return regex;}
function getNativeFlags(regex){return(regex.global?"g":"")+
(regex.ignoreCase?"i":"")+
(regex.multiline?"m":"")+
(regex.extended?"x":"")+
(regex.sticky?"y":"");}
function copy(regex,addFlags,removeFlags){if(!self.isRegExp(regex)){throw new TypeError("type RegExp expected");}
var flags=nativ.replace.call(getNativeFlags(regex)+(addFlags||""),duplicateFlags,"");if(removeFlags){flags=nativ.replace.call(flags,new RegExp("["+removeFlags+"]+","g"),"");}
if(regex.xregexp&&!regex.xregexp.isNative){regex=augment(self(regex.source,flags),regex.xregexp.captureNames?regex.xregexp.captureNames.slice(0):null);}else{regex=augment(new RegExp(regex.source,flags),null,true);}
return regex;}
function lastIndexOf(array,value){var i=array.length;if(Array.prototype.lastIndexOf){return array.lastIndexOf(value);}
while(i--){if(array[i]===value){return i;}}
return-1;}
function isType(value,type){return Object.prototype.toString.call(value).toLowerCase()==="[object "+type+"]";}
function prepareOptions(value){value=value||{};if(value==="all"||value.all){value={natives:true,extensibility:true};}else if(isType(value,"string")){value=self.forEach(value,/[^\s,]+/,function(m){this[m]=true;},{});}
return value;}
function runTokens(pattern,pos,scope,context){var i=tokens.length,result=null,match,t;isInsideConstructor=true;try{while(i--){t=tokens[i];if((t.scope==="all"||t.scope===scope)&&(!t.trigger||t.trigger.call(context))){t.pattern.lastIndex=pos;match=fixed.exec.call(t.pattern,pattern);if(match&&match.index===pos){result={output:t.handler.call(context,match,scope),match:match};break;}}}}catch(err){throw err;}finally{isInsideConstructor=false;}
return result;}
function setExtensibility(on){self.addToken=addToken[on?"on":"off"];features.extensibility=on;}
function setNatives(on){RegExp.prototype.exec=(on?fixed:nativ).exec;RegExp.prototype.test=(on?fixed:nativ).test;String.prototype.match=(on?fixed:nativ).match;String.prototype.replace=(on?fixed:nativ).replace;String.prototype.split=(on?fixed:nativ).split;features.natives=on;}
self=function(pattern,flags){if(self.isRegExp(pattern)){if(flags!==undef){throw new TypeError("can't supply flags when constructing one RegExp from another");}
return copy(pattern);}
if(isInsideConstructor){throw new Error("can't call the XRegExp constructor within token definition functions");}
var output=[],scope=defaultScope,tokenContext={hasNamedCapture:false,captureNames:[],hasFlag:function(flag){return flags.indexOf(flag)>-1;}},pos=0,tokenResult,match,chr;pattern=pattern===undef?"":String(pattern);flags=flags===undef?"":String(flags);if(nativ.match.call(flags,duplicateFlags)){throw new SyntaxError("invalid duplicate regular expression flag");}
pattern=nativ.replace.call(pattern,/^\(\?([\w$]+)\)/,function($0,$1){if(nativ.test.call(/[gy]/,$1)){throw new SyntaxError("can't use flag g or y in mode modifier");}
flags=nativ.replace.call(flags+$1,duplicateFlags,"");return"";});self.forEach(flags,/[\s\S]/,function(m){if(registeredFlags.indexOf(m[0])<0){throw new SyntaxError("invalid regular expression flag "+m[0]);}});while(pos<pattern.length){tokenResult=runTokens(pattern,pos,scope,tokenContext);if(tokenResult){output.push(tokenResult.output);pos+=(tokenResult.match[0].length||1);}else{match=nativ.exec.call(nativeTokens[scope],pattern.slice(pos));if(match){output.push(match[0]);pos+=match[0].length;}else{chr=pattern.charAt(pos);if(chr==="["){scope=classScope;}else if(chr==="]"){scope=defaultScope;}
output.push(chr);++pos;}}}
return augment(new RegExp(output.join(""),nativ.replace.call(flags,/[^gimy]+/g,"")),tokenContext.hasNamedCapture?tokenContext.captureNames:null);};addToken={on:function(regex,handler,options){options=options||{};if(regex){tokens.push({pattern:copy(regex,"g"+(hasNativeY?"y":"")),handler:handler,scope:options.scope||defaultScope,trigger:options.trigger||null});}
if(options.customFlags){registeredFlags=nativ.replace.call(registeredFlags+options.customFlags,duplicateFlags,"");}},off:function(){throw new Error("extensibility must be installed before using addToken");}};self.addToken=addToken.off;self.cache=function(pattern,flags){var key=pattern+"/"+(flags||"");return cache[key]||(cache[key]=self(pattern,flags));};self.escape=function(str){return nativ.replace.call(str,/[-[\]{}()*+?.,\\^$|#\s]/g,"\\$&");};self.exec=function(str,regex,pos,sticky){var r2=copy(regex,"g"+(sticky&&hasNativeY?"y":""),(sticky===false?"y":"")),match;r2.lastIndex=pos=pos||0;match=fixed.exec.call(r2,str);if(sticky&&match&&match.index!==pos){match=null;}
if(regex.global){regex.lastIndex=match?r2.lastIndex:0;}
return match;};self.forEach=function(str,regex,callback,context){var pos=0,i=-1,match;while((match=self.exec(str,regex,pos))){callback.call(context,match,++i,str,regex);pos=match.index+(match[0].length||1);}
return context;};self.globalize=function(regex){return copy(regex,"g");};self.install=function(options){options=prepareOptions(options);if(!features.natives&&options.natives){setNatives(true);}
if(!features.extensibility&&options.extensibility){setExtensibility(true);}};self.isInstalled=function(feature){return!!(features[feature]);};self.isRegExp=function(value){return isType(value,"regexp");};self.matchChain=function(str,chain){return(function recurseChain(values,level){var item=chain[level].regex?chain[level]:{regex:chain[level]},matches=[],addMatch=function(match){matches.push(item.backref?(match[item.backref]||""):match[0]);},i;for(i=0;i<values.length;++i){self.forEach(values[i],item.regex,addMatch);}
return((level===chain.length-1)||!matches.length)?matches:recurseChain(matches,level+1);}([str],0));};self.replace=function(str,search,replacement,scope){var isRegex=self.isRegExp(search),search2=search,result;if(isRegex){if(scope===undef&&search.global){scope="all";}
search2=copy(search,scope==="all"?"g":"",scope==="all"?"":"g");}else if(scope==="all"){search2=new RegExp(self.escape(String(search)),"g");}
result=fixed.replace.call(String(str),search2,replacement);if(isRegex&&search.global){search.lastIndex=0;}
return result;};self.split=function(str,separator,limit){return fixed.split.call(str,separator,limit);};self.test=function(str,regex,pos,sticky){return!!self.exec(str,regex,pos,sticky);};self.uninstall=function(options){options=prepareOptions(options);if(features.natives&&options.natives){setNatives(false);}
if(features.extensibility&&options.extensibility){setExtensibility(false);}};self.union=function(patterns,flags){var parts=/(\()(?!\?)|\\([1-9]\d*)|\\[\s\S]|\[(?:[^\\\]]|\\[\s\S])*]/g,numCaptures=0,numPriorCaptures,captureNames,rewrite=function(match,paren,backref){var name=captureNames[numCaptures-numPriorCaptures];if(paren){++numCaptures;if(name){return"(?<"+name+">";}}else if(backref){return"\\"+(+backref+numPriorCaptures);}
return match;},output=[],pattern,i;if(!(isType(patterns,"array")&&patterns.length)){throw new TypeError("patterns must be a nonempty array");}
for(i=0;i<patterns.length;++i){pattern=patterns[i];if(self.isRegExp(pattern)){numPriorCaptures=numCaptures;captureNames=(pattern.xregexp&&pattern.xregexp.captureNames)||[];output.push(self(pattern.source).source.replace(parts,rewrite));}else{output.push(self.escape(pattern));}}
return self(output.join("|"),flags);};self.version="2.0.0";fixed.exec=function(str){var match,name,r2,origLastIndex,i;if(!this.global){origLastIndex=this.lastIndex;}
match=nativ.exec.apply(this,arguments);if(match){if(!compliantExecNpcg&&match.length>1&&lastIndexOf(match,"")>-1){r2=new RegExp(this.source,nativ.replace.call(getNativeFlags(this),"g",""));nativ.replace.call(String(str).slice(match.index),r2,function(){var i;for(i=1;i<arguments.length-2;++i){if(arguments[i]===undef){match[i]=undef;}}});}
if(this.xregexp&&this.xregexp.captureNames){for(i=1;i<match.length;++i){name=this.xregexp.captureNames[i-1];if(name){match[name]=match[i];}}}
if(this.global&&!match[0].length&&(this.lastIndex>match.index)){this.lastIndex=match.index;}}
if(!this.global){this.lastIndex=origLastIndex;}
return match;};fixed.test=function(str){return!!fixed.exec.call(this,str);};fixed.match=function(regex){if(!self.isRegExp(regex)){regex=new RegExp(regex);}else if(regex.global){var result=nativ.match.apply(this,arguments);regex.lastIndex=0;return result;}
return fixed.exec.call(regex,this);};fixed.replace=function(search,replacement){var isRegex=self.isRegExp(search),captureNames,result,str,origLastIndex;if(isRegex){if(search.xregexp){captureNames=search.xregexp.captureNames;}
if(!search.global){origLastIndex=search.lastIndex;}}else{search+="";}
if(isType(replacement,"function")){result=nativ.replace.call(String(this),search,function(){var args=arguments,i;if(captureNames){args[0]=new String(args[0]);for(i=0;i<captureNames.length;++i){if(captureNames[i]){args[0][captureNames[i]]=args[i+1];}}}
if(isRegex&&search.global){search.lastIndex=args[args.length-2]+args[0].length;}
return replacement.apply(null,args);});}else{str=String(this);result=nativ.replace.call(str,search,function(){var args=arguments;return nativ.replace.call(String(replacement),replacementToken,function($0,$1,$2){var n;if($1){n=+$1;if(n<=args.length-3){return args[n]||"";}
n=captureNames?lastIndexOf(captureNames,$1):-1;if(n<0){throw new SyntaxError("backreference to undefined group "+$0);}
return args[n+1]||"";}
if($2==="$")return"$";if($2==="&"||+$2===0)return args[0];if($2==="`")return args[args.length-1].slice(0,args[args.length-2]);if($2==="'")return args[args.length-1].slice(args[args.length-2]+args[0].length);$2=+$2;if(!isNaN($2)){if($2>args.length-3){throw new SyntaxError("backreference to undefined group "+$0);}
return args[$2]||"";}
throw new SyntaxError("invalid token "+$0);});});}
if(isRegex){if(search.global){search.lastIndex=0;}else{search.lastIndex=origLastIndex;}}
return result;};fixed.split=function(separator,limit){if(!self.isRegExp(separator)){return nativ.split.apply(this,arguments);}
var str=String(this),origLastIndex=separator.lastIndex,output=[],lastLastIndex=0,lastLength;limit=(limit===undef?-1:limit)>>>0;self.forEach(str,separator,function(match){if((match.index+match[0].length)>lastLastIndex){output.push(str.slice(lastLastIndex,match.index));if(match.length>1&&match.index<str.length){Array.prototype.push.apply(output,match.slice(1));}
lastLength=match[0].length;lastLastIndex=match.index+lastLength;}});if(lastLastIndex===str.length){if(!nativ.test.call(separator,"")||lastLength){output.push("");}}else{output.push(str.slice(lastLastIndex));}
separator.lastIndex=origLastIndex;return output.length>limit?output.slice(0,limit):output;};add=addToken.on;add(/\\([ABCE-RTUVXYZaeg-mopqyz]|c(?![A-Za-z])|u(?![\dA-Fa-f]{4})|x(?![\dA-Fa-f]{2}))/,function(match,scope){if(match[1]==="B"&&scope===defaultScope){return match[0];}
throw new SyntaxError("invalid escape "+match[0]);},{scope:"all"});add(/\[(\^?)]/,function(match){return match[1]?"[\\s\\S]":"\\b\\B";});add(/(?:\(\?#[^)]*\))+/,function(match){return nativ.test.call(quantifier,match.input.slice(match.index+match[0].length))?"":"(?:)";});add(/\\k<([\w$]+)>/,function(match){var index=isNaN(match[1])?(lastIndexOf(this.captureNames,match[1])+1):+match[1],endIndex=match.index+match[0].length;if(!index||index>this.captureNames.length){throw new SyntaxError("backreference to undefined group "+match[0]);}
return"\\"+index+(endIndex===match.input.length||isNaN(match.input.charAt(endIndex))?"":"(?:)");});add(/(?:\s+|#.*)+/,function(match){return nativ.test.call(quantifier,match.input.slice(match.index+match[0].length))?"":"(?:)";},{trigger:function(){return this.hasFlag("x");},customFlags:"x"});add(/\./,function(){return"[\\s\\S]";},{trigger:function(){return this.hasFlag("s");},customFlags:"s"});add(/\(\?P?<([\w$]+)>/,function(match){if(!isNaN(match[1])){throw new SyntaxError("can't use integer as capture name "+match[0]);}
this.captureNames.push(match[1]);this.hasNamedCapture=true;return"(";});add(/\\(\d+)/,function(match,scope){if(!(scope===defaultScope&&/^[1-9]/.test(match[1])&&+match[1]<=this.captureNames.length)&&match[1]!=="0"){throw new SyntaxError("can't use octal escape or backreference to undefined group "+match[0]);}
return match[0];},{scope:"all"});add(/\((?!\?)/,function(){if(this.hasFlag("n")){return"(?:";}
this.captureNames.push(null);return"(";},{customFlags:"n"});if(typeof exports!=="undefined"){exports.XRegExp=self;}
return self;}());if(typeof(SyntaxHighlighter)=='undefined')var SyntaxHighlighter=function(){if(typeof(require)!='undefined'&&typeof(XRegExp)=='undefined')
{XRegExp=require('xregexp').XRegExp;}
var sh={defaults:{'class-name':'','first-line':1,'pad-line-numbers':false,'highlight':null,'title':null,'smart-tabs':true,'tab-size':4,'gutter':true,'toolbar':true,'quick-code':true,'collapse':false,'auto-links':true,'light':false,'unindent':true,'html-script':false},config:{space:'&nbsp;',useScriptTags:true,bloggerMode:false,stripBrs:false,tagName:'pre',strings:{expandSource:'expand source',help:'?',alert:'SyntaxHighlighter\n\n',noBrush:'Can\'t find brush for: ',brushNotHtmlScript:'Brush wasn\'t configured for html-script option: ',aboutDialog:'<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\"><html xmlns=\"http://www.w3.org/1999/xhtml\"><head><meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" /><title>About SyntaxHighlighter</title></head><body style=\"font-family:Geneva,Arial,Helvetica,sans-serif;background-color:#fff;color:#000;font-size:1em;text-align:center;\"><div style=\"text-align:center;margin-top:1.5em;\"><div style=\"font-size:xx-large;\">SyntaxHighlighter</div><div style=\"font-size:.75em;margin-bottom:3em;\"><div>version 3.0.9 (Thu, 04 Dec 2014 12:32:21 GMT)</div><div><a href=\"http://alexgorbatchev.com/SyntaxHighlighter\" target=\"_blank\" style=\"color:#005896\">http://alexgorbatchev.com/SyntaxHighlighter</a></div><div>JavaScript code syntax highlighter.</div><div>Copyright 2004-2013 Alex Gorbatchev.</div></div><div>If you like this script, please <a href=\"https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=2930402\" style=\"color:#005896\">donate</a> to <br/>keep development active!</div></div></body></html>'}},vars:{highlighters:{}},brushes:{},regexLib:{multiLineCComments:XRegExp('/\\*.*?\\*/','gs'),singleLineCComments:/\/\/.*$/gm,singleLinePerlComments:/#.*$/gm,doubleQuotedString:/"([^\\"\n]|\\.)*"/g,singleQuotedString:/'([^\\'\n]|\\.)*'/g,multiLineDoubleQuotedString:XRegExp('"([^\\\\"]|\\\\.)*"','gs'),multiLineSingleQuotedString:XRegExp("'([^\\\\']|\\\\.)*'",'gs'),xmlComments:XRegExp('(&lt;|<)!--.*?--(&gt;|>)','gs'),url:/\w+:\/\/[\w-.\/?%&=:@;#]*/g,phpScriptTags:{left:/(&lt;|<)\?(?:=|php)?/g,right:/\?(&gt;|>)/g,'eof':true},aspScriptTags:{left:/(&lt;|<)%=?/g,right:/%(&gt;|>)/g},scriptScriptTags:{left:/(&lt;|<)\s*script.*?(&gt;|>)/gi,right:/(&lt;|<)\/\s*script\s*(&gt;|>)/gi}},toolbar:{getHtml:function(highlighter)
{var html='<div class="toolbar">',items=sh.toolbar.items,list=items.list;function defaultGetHtml(highlighter,name)
{return sh.toolbar.getButtonHtml(highlighter,name,sh.config.strings[name]);}
for(var i=0,l=list.length;i<l;i++)
{html+=(items[list[i]].getHtml||defaultGetHtml)(highlighter,list[i]);}
html+='</div>';return html;},getButtonHtml:function(highlighter,commandName,label)
{commandName=escapeHtml(commandName);return'<span><a href="#" class="toolbar_item'
+' command_'+commandName
+' '+commandName
+'">'+escapeHtml(label)+'</a></span>';},handler:function(e)
{var target=e.target,className=target.className||'';function getValue(name)
{var r=new RegExp(name+'_(\\w+)'),match=r.exec(className);return match?match[1]:null;}
var highlighter=getHighlighterById(findParentElement(target,'.syntaxhighlighter').id),commandName=getValue('command');if(highlighter&&commandName)
sh.toolbar.items[commandName].execute(highlighter);e.preventDefault();},items:{list:['expandSource','help'],expandSource:{getHtml:function(highlighter)
{if(highlighter.getParam('collapse')!=true)
return'';var title=highlighter.getParam('title');return sh.toolbar.getButtonHtml(highlighter,'expandSource',title?title:sh.config.strings.expandSource);},execute:function(highlighter)
{var div=getHighlighterDivById(highlighter.id);removeClass(div,'collapsed');}},help:{execute:function(highlighter)
{var wnd=popup('','_blank',500,250,'scrollbars=0'),doc=wnd.document;doc.write(sh.config.strings.aboutDialog);doc.close();wnd.focus();}}}},findElements:function(globalParams,element)
{var elements=element?[element]:toArray(document.getElementsByTagName(sh.config.tagName)),conf=sh.config,result=[];if(conf.useScriptTags)
elements=elements.concat(getSyntaxHighlighterScriptTags());if(elements.length===0)
return result;for(var i=0,l=elements.length;i<l;i++)
{var item={target:elements[i],params:merge(globalParams,parseParams(elements[i].className))};if(item.params['brush']==null)
continue;result.push(item);}
return result;},highlight:function(globalParams,element)
{var elements=this.findElements(globalParams,element),propertyName='innerHTML',highlighter=null,conf=sh.config;if(elements.length===0)
return;for(var i=0,l=elements.length;i<l;i++)
{var element=elements[i],target=element.target,params=element.params,brushName=params.brush,code;if(brushName==null)
continue;if(params['html-script']=='true'||sh.defaults['html-script']==true)
{highlighter=new sh.HtmlScript(brushName);brushName='htmlscript';}
else
{var brush=findBrush(brushName);if(brush)
highlighter=new brush();else
continue;}
code=target[propertyName];if(conf.useScriptTags)
code=stripCData(code);if((target.title||'')!='')
params.title=target.title;params['brush']=brushName;highlighter.init(params);element=highlighter.getDiv(code);if((target.id||'')!='')
element.id=target.id;target.parentNode.replaceChild(element,target);}},all:function(params)
{attachEvent(window,'load',function(){sh.highlight(params);});}};function escapeHtml(html)
{return document.createElement('div').appendChild(document.createTextNode(html)).parentNode.innerHTML.replace(/"/g,'&quot;');};function hasClass(target,className)
{return target.className.indexOf(className)!=-1;};function addClass(target,className)
{if(!hasClass(target,className))
target.className+=' '+className;};function removeClass(target,className)
{target.className=target.className.replace(className,'');};function toArray(source)
{var result=[];for(var i=0,l=source.length;i<l;i++)
result.push(source[i]);return result;};function splitLines(block)
{return block.split(/\r?\n/);}
function getHighlighterId(id)
{var prefix='highlighter_';return id.indexOf(prefix)==0?id:prefix+id;};function getHighlighterById(id)
{return sh.vars.highlighters[getHighlighterId(id)];};function getHighlighterDivById(id)
{return document.getElementById(getHighlighterId(id));};function storeHighlighter(highlighter)
{sh.vars.highlighters[getHighlighterId(highlighter.id)]=highlighter;};function findElement(target,search,reverse)
{if(target==null)
return null;var nodes=reverse!=true?target.childNodes:[target.parentNode],propertyToFind={'#':'id','.':'className'}[search.substr(0,1)]||'nodeName',expectedValue,found;expectedValue=propertyToFind!='nodeName'?search.substr(1):search.toUpperCase();if((target[propertyToFind]||'').indexOf(expectedValue)!=-1)
return target;for(var i=0,l=nodes.length;nodes&&i<l&&found==null;i++)
found=findElement(nodes[i],search,reverse);return found;};function findParentElement(target,className)
{return findElement(target,className,true);};function indexOf(array,searchElement,fromIndex)
{fromIndex=Math.max(fromIndex||0,0);for(var i=fromIndex,l=array.length;i<l;i++)
if(array[i]==searchElement)
return i;return-1;};function guid(prefix)
{return(prefix||'')+Math.round(Math.random()*1000000).toString();};function merge(obj1,obj2)
{var result={},name;for(name in obj1)
result[name]=obj1[name];for(name in obj2)
result[name]=obj2[name];return result;};function toBoolean(value)
{var result={"true":true,"false":false}[value];return result==null?value:result;};function popup(url,name,width,height,options)
{var x=(screen.width-width)/ 2,y=(screen.height-height)/ 2;options+=', left='+x+', top='+y+', width='+width+', height='+height;options=options.replace(/^,/,'');var win=window.open(url,name,options);win.focus();return win;};function attachEvent(obj,type,func,scope)
{function handler(e)
{e=e||window.event;if(!e.target)
{e.target=e.srcElement;e.preventDefault=function()
{this.returnValue=false;};}
func.call(scope||window,e);};if(obj.attachEvent)
{obj.attachEvent('on'+type,handler);}
else
{obj.addEventListener(type,handler,false);}};function alert(str)
{window.alert(sh.config.strings.alert+str);};function findBrush(alias,showAlert)
{var brush=sh.brushes[alias];if(brush!==undefined){return brush;}
for(brush in sh.brushes)
{var info=sh.brushes[brush],aliases=info.aliases;if(aliases==null)
continue;info.brushName=brush.toLowerCase();for(var i=0,l=aliases.length;i<l;i++)
sh.brushes[aliases[i]]=info;}
brush=sh.brushes[alias];if(brush===undefined){sh.brushes[alias]=null;if(showAlert){alert(sh.config.strings.noBrush+alias);}}
return brush;};function eachLine(str,callback)
{var lines=splitLines(str);for(var i=0,l=lines.length;i<l;i++)
lines[i]=callback(lines[i],i);return lines.join('\r\n');};function trimFirstAndLastLines(str)
{return str.replace(/^[ ]*[\n]+|[\n]*[ ]*$/g,'');};function parseParams(str)
{var match,result={},arrayRegex=XRegExp("^\\[(?<values>(.*?))\\]$"),pos=0,regex=XRegExp("(?<name>[\\w-]+)"+"\\s*:\\s*"+"(?<value>"+"[\\w%#-]+|"+"\\[.*?\\]|"+'".*?"|'+"'.*?'"+")\\s*;?","g");while((match=XRegExp.exec(str,regex,pos))!=null)
{var value=match.value.replace(/^['"]|['"]$/g,'');if(value!=null&&arrayRegex.test(value))
{var m=XRegExp.exec(value,arrayRegex);value=m.values.length>0?m.values.split(/\s*,\s*/):[];}
result[match.name]=value;pos=match.index+match[0].length;}
return result;};function wrapLinesWithCode(str,css)
{if(str==null||str.length==0||str=='\n')
return str;str=str.replace(/</g,'&lt;');str=str.replace(/ {2,}/g,function(m)
{var spaces='';for(var i=0,l=m.length;i<l-1;i++)
spaces+=sh.config.space;return spaces+' ';});if(css!=null)
str=eachLine(str,function(line)
{if(line.length==0)
return'';var spaces='';line=line.replace(/^(&nbsp;| )+/,function(s)
{spaces=s;return'';});if(line.length==0)
return spaces;return spaces+'<code class="'+css+'">'+line+'</code>';});return str;};function padNumber(number,length)
{var result=number.toString();while(result.length<length)
result='0'+result;return result;};function processTabs(code,tabSize)
{var tab='';for(var i=0;i<tabSize;i++)
tab+=' ';return code.replace(/\t/g,tab);};function processSmartTabs(code,tabSize)
{var lines=splitLines(code),tab='\t',spaces='';for(var i=0;i<50;i++)
spaces+='                    ';function insertSpaces(line,pos,count)
{return line.substr(0,pos)
+spaces.substr(0,count)
+line.substr(pos+1,line.length);};code=eachLine(code,function(line)
{if(line.indexOf(tab)==-1)
return line;var pos=0;while((pos=line.indexOf(tab))!=-1)
{var spaces=tabSize-pos%tabSize;line=insertSpaces(line,pos,spaces);}
return line;});return code;};function fixInputString(str)
{var br=/<br\s*\/?>|&lt;br\s*\/?&gt;/gi;if(sh.config.bloggerMode==true)
str=str.replace(br,'\n');if(sh.config.stripBrs==true)
str=str.replace(br,'');return str;};function trim(str)
{return str.replace(/^\s+|\s+$/g,'');};function unindent(str)
{var lines=splitLines(fixInputString(str)),indents=new Array(),regex=/^\s*/,min=1000;for(var i=0,l=lines.length;i<l&&min>0;i++)
{var line=lines[i];if(trim(line).length==0)
continue;var matches=regex.exec(line);if(matches==null)
return str;min=Math.min(matches[0].length,min);}
if(min>0)
for(var i=0,l=lines.length;i<l;i++)
lines[i]=lines[i].substr(min);return lines.join('\n');};function matchesSortCallback(m1,m2)
{if(m1.index<m2.index)
return-1;else if(m1.index>m2.index)
return 1;else
{if(m1.length<m2.length)
return-1;else if(m1.length>m2.length)
return 1;}
return 0;};function getMatches(code,regexInfo)
{function defaultAdd(match,regexInfo)
{return match[0];};var index=0,match=null,matches=[],func=regexInfo.func?regexInfo.func:defaultAdd
pos=0;while((match=XRegExp.exec(code,regexInfo.regex,pos))!=null)
{var resultMatch=func(match,regexInfo);if(typeof(resultMatch)=='string')
resultMatch=[new sh.Match(resultMatch,match.index,regexInfo.css)];matches=matches.concat(resultMatch);pos=match.index+match[0].length;}
return matches;};function processUrls(code)
{var gt=/(.*)((&gt;|&lt;).*)/;return code.replace(sh.regexLib.url,function(m)
{var suffix='',match=null;if(match=gt.exec(m))
{m=match[1];suffix=match[2];}
return'<a href="'+m+'">'+m+'</a>'+suffix;});};function getSyntaxHighlighterScriptTags()
{var tags=document.getElementsByTagName('script'),result=[];for(var i=0,l=tags.length;i<l;i++)
if(tags[i].type=='syntaxhighlighter')
result.push(tags[i]);return result;};function stripCData(original)
{var left='<![CDATA[',right=']]>',copy=trim(original),changed=false,leftLength=left.length,rightLength=right.length;if(copy.indexOf(left)==0)
{copy=copy.substring(leftLength);changed=true;}
var copyLength=copy.length;if(copy.indexOf(right)==copyLength-rightLength)
{copy=copy.substring(0,copyLength-rightLength);changed=true;}
return changed?copy:original;};function quickCodeHandler(e)
{var target=e.target,highlighterDiv=findParentElement(target,'.syntaxhighlighter'),container=findParentElement(target,'.container'),textarea=document.createElement('textarea'),highlighter;if(!container||!highlighterDiv||findElement(container,'textarea'))
return;highlighter=getHighlighterById(highlighterDiv.id);addClass(highlighterDiv,'source');var lines=container.childNodes,code=[];for(var i=0,l=lines.length;i<l;i++)
code.push(lines[i].innerText||lines[i].textContent);code=code.join('\r');code=code.replace(/\u00a0/g," ");textarea.appendChild(document.createTextNode(code));container.appendChild(textarea);textarea.focus();textarea.select();attachEvent(textarea,'blur',function(e)
{textarea.parentNode.removeChild(textarea);removeClass(highlighterDiv,'source');});};sh.Match=function(value,index,css)
{this.value=value;this.index=index;this.length=value.length;this.css=css;this.brushName=null;};sh.Match.prototype.toString=function()
{return this.value;};sh.HtmlScript=function(scriptBrushName)
{var brushClass=findBrush(scriptBrushName),scriptBrush,xmlBrush=new sh.brushes.Xml(),bracketsRegex=null,ref=this,methodsToExpose='getDiv getHtml init'.split(' ');if(brushClass==null)
return;scriptBrush=new brushClass();for(var i=0,l=methodsToExpose.length;i<l;i++)
(function(){var name=methodsToExpose[i];ref[name]=function()
{return xmlBrush[name].apply(xmlBrush,arguments);};})();if(scriptBrush.htmlScript==null)
{alert(sh.config.strings.brushNotHtmlScript+scriptBrushName);return;}
xmlBrush.regexList.push({regex:scriptBrush.htmlScript.code,func:process});function offsetMatches(matches,offset)
{for(var j=0,l=matches.length;j<l;j++)
matches[j].index+=offset;}
function process(match,info)
{var code=match.code,matches=[],regexList=scriptBrush.regexList,offset=match.index+match.left.length,htmlScript=scriptBrush.htmlScript,result;for(var i=0,l=regexList.length;i<l;i++)
{result=getMatches(code,regexList[i]);offsetMatches(result,offset);matches=matches.concat(result);}
if(htmlScript.left!=null&&match.left!=null)
{result=getMatches(match.left,htmlScript.left);offsetMatches(result,match.index);matches=matches.concat(result);}
if(htmlScript.right!=null&&match.right!=null)
{result=getMatches(match.right,htmlScript.right);offsetMatches(result,match.index+match[0].lastIndexOf(match.right));matches=matches.concat(result);}
for(var j=0,l=matches.length;j<l;j++)
matches[j].brushName=brushClass.brushName;return matches;}};sh.Highlighter=function()
{};sh.Highlighter.prototype={getParam:function(name,defaultValue)
{var result=this.params[name];return toBoolean(result==null?defaultValue:result);},create:function(name)
{return document.createElement(name);},findMatches:function(regexList,code)
{var result=[];if(regexList!=null)
for(var i=0,l=regexList.length;i<l;i++)
if(typeof(regexList[i])=="object")
result=result.concat(getMatches(code,regexList[i]));return this.removeNestedMatches(result.sort(matchesSortCallback));},removeNestedMatches:function(matches)
{for(var i=0,l=matches.length;i<l;i++)
{if(matches[i]===null)
continue;var itemI=matches[i],itemIEndPos=itemI.index+itemI.length;for(var j=i+1,l=matches.length;j<l&&matches[i]!==null;j++)
{var itemJ=matches[j];if(itemJ===null)
continue;else if(itemJ.index>itemIEndPos)
break;else if(itemJ.index==itemI.index&&itemJ.length>itemI.length)
matches[i]=null;else if(itemJ.index>=itemI.index&&itemJ.index<itemIEndPos)
matches[j]=null;}}
return matches;},figureOutLineNumbers:function(code)
{var lines=[],firstLine=parseInt(this.getParam('first-line'));eachLine(code,function(line,index)
{lines.push(index+firstLine);});return lines;},isLineHighlighted:function(lineNumber)
{var list=this.getParam('highlight',[]);if(typeof(list)!='object'&&list.push==null)
list=[list];return indexOf(list,lineNumber.toString())!=-1;},getLineHtml:function(lineIndex,lineNumber,code)
{var classes=['line','number'+lineNumber,'index'+lineIndex,'alt'+(lineNumber%2==0?1:2).toString()];if(this.isLineHighlighted(lineNumber))
classes.push('highlighted');if(lineNumber==0)
classes.push('break');return'<div class="'+classes.join(' ')+'">'+code+'</div>';},getLineNumbersHtml:function(code,lineNumbers)
{var html='',count=splitLines(code).length,firstLine=parseInt(this.getParam('first-line')),pad=this.getParam('pad-line-numbers');if(pad==true)
pad=(firstLine+count-1).toString().length;else if(isNaN(pad)==true)
pad=0;for(var i=0;i<count;i++)
{var lineNumber=lineNumbers?lineNumbers[i]:firstLine+i,code=lineNumber==0?sh.config.space:padNumber(lineNumber,pad);html+=this.getLineHtml(i,lineNumber,code);}
return html;},getCodeLinesHtml:function(html,lineNumbers)
{html=trim(html);var lines=splitLines(html),padLength=this.getParam('pad-line-numbers'),firstLine=parseInt(this.getParam('first-line')),html='',brushName=this.getParam('brush');for(var i=0,l=lines.length;i<l;i++)
{var line=lines[i],indent=/^(&nbsp;|\s)+/.exec(line),spaces=null,lineNumber=lineNumbers?lineNumbers[i]:firstLine+i;;if(indent!=null)
{spaces=indent[0].toString();line=line.substr(spaces.length);spaces=spaces.replace(' ',sh.config.space);}
line=trim(line);if(line.length==0)
line=sh.config.space;html+=this.getLineHtml(i,lineNumber,(spaces!=null?'<code class="'+brushName+' spaces">'+spaces+'</code>':'')+line);}
return html;},getTitleHtml:function(title)
{return title?'<caption>'+escapeHtml(title)+'</caption>':'';},getMatchesHtml:function(code,matches)
{var pos=0,result='',brushName=this.getParam('brush','');function getBrushNameCss(match)
{var result=match?(match.brushName||brushName):brushName;return result?result+' ':'';};for(var i=0,l=matches.length;i<l;i++)
{var match=matches[i],matchBrushName;if(match===null||match.length===0)
continue;matchBrushName=getBrushNameCss(match);result+=wrapLinesWithCode(code.substr(pos,match.index-pos),matchBrushName+'plain')
+wrapLinesWithCode(match.value,matchBrushName+match.css);pos=match.index+match.length+(match.offset||0);}
result+=wrapLinesWithCode(code.substr(pos),getBrushNameCss()+'plain');return result;},getHtml:function(code)
{var html='',classes=['syntaxhighlighter'],tabSize,matches,lineNumbers;if(this.getParam('light')==true)
this.params.toolbar=this.params.gutter=false;className='syntaxhighlighter';if(this.getParam('collapse')==true)
classes.push('collapsed');if((gutter=this.getParam('gutter'))==false)
classes.push('nogutter');classes.push(this.getParam('class-name'));classes.push(this.getParam('brush'));code=trimFirstAndLastLines(code).replace(/\r/g,' ');tabSize=this.getParam('tab-size');code=this.getParam('smart-tabs')==true?processSmartTabs(code,tabSize):processTabs(code,tabSize);if(this.getParam('unindent'))
code=unindent(code);if(gutter)
lineNumbers=this.figureOutLineNumbers(code);matches=this.findMatches(this.regexList,code);html=this.getMatchesHtml(code,matches);html=this.getCodeLinesHtml(html,lineNumbers);if(this.getParam('auto-links'))
html=processUrls(html);if(typeof(navigator)!='undefined'&&navigator.userAgent&&navigator.userAgent.match(/MSIE/))
classes.push('ie');html='<div id="'+getHighlighterId(this.id)+'" class="'+escapeHtml(classes.join(' '))+'">'
+(this.getParam('toolbar')?sh.toolbar.getHtml(this):'')
+'<table border="0" cellpadding="0" cellspacing="0">'
+this.getTitleHtml(this.getParam('title'))
+'<tbody>'
+'<tr>'
+(gutter?'<td class="gutter">'+this.getLineNumbersHtml(code)+'</td>':'')
+'<td class="code">'
+'<div class="container">'
+html
+'</div>'
+'</td>'
+'</tr>'
+'</tbody>'
+'</table>'
+'</div>';return html;},getDiv:function(code)
{if(code===null)
code='';this.code=code;var div=this.create('div');div.innerHTML=this.getHtml(code);if(this.getParam('toolbar'))
attachEvent(findElement(div,'.toolbar'),'click',sh.toolbar.handler);if(this.getParam('quick-code'))
attachEvent(findElement(div,'.code'),'dblclick',quickCodeHandler);return div;},init:function(params)
{this.id=guid();storeHighlighter(this);this.params=merge(sh.defaults,params||{})
if(this.getParam('light')==true)
this.params.toolbar=this.params.gutter=false;},getKeywords:function(str)
{str=str.replace(/^\s+|\s+$/g,'').replace(/\s+/g,'|');return'\\b(?:'+str+')\\b';},forHtmlScript:function(regexGroup)
{var regex={'end':regexGroup.right.source};if(regexGroup.eof)
regex.end="(?:(?:"+regex.end+")|$)";this.htmlScript={left:{regex:regexGroup.left,css:'script'},right:{regex:regexGroup.right,css:'script'},code:XRegExp("(?<left>"+regexGroup.left.source+")"+"(?<code>.*?)"+"(?<right>"+regex.end+")","sgi")};}};return sh;}();typeof(exports)!='undefined'?exports.SyntaxHighlighter=SyntaxHighlighter:null;(function(){var sh=SyntaxHighlighter;sh.autoloader=function()
{var list=arguments,elements=sh.findElements(),brushes={},scripts={},all=SyntaxHighlighter.all,allCalled=false,allParams=null,i;SyntaxHighlighter.all=function(params)
{allParams=params;allCalled=true;};function addBrush(aliases,url)
{for(var i=0;i<aliases.length;i++)
brushes[aliases[i]]=url;};function getAliases(item)
{return item.pop?item:item.split(/\s+/);}
for(i=0;i<list.length;i++)
{var aliases=getAliases(list[i]),url=aliases.pop();addBrush(aliases,url);}
for(i=0;i<elements.length;i++)
{var url=brushes[elements[i].params.brush];if(url&&scripts[url]===undefined)
{if(elements[i].params['html-script']==='true')
{if(scripts[brushes['xml']]===undefined){loadScript(brushes['xml']);scripts[url]=false;}}
scripts[url]=false;loadScript(url);}}
function loadScript(url)
{var script=document.createElement('script'),done=false;script.src=url;script.type='text/javascript';script.language='javascript';script.onload=script.onreadystatechange=function()
{if(!done&&(!this.readyState||this.readyState=='loaded'||this.readyState=='complete'))
{done=true;scripts[url]=true;checkAll();script.onload=script.onreadystatechange=null;script.parentNode.removeChild(script);}};document.body.appendChild(script);};function checkAll()
{for(var url in scripts)
if(scripts[url]==false)
return;if(allCalled)
SyntaxHighlighter.highlight(allParams);};};})();var yash3_path="/index.php?pf=yash3/syntaxhighlighter/js/";function shGetPath()
{var args=arguments,result=[];for(var i=0;i<args.length;i++)
result.push(args[i].replace('@',yash3_path));return result;};SyntaxHighlighter.autoloader.apply(null,shGetPath('applescript            @shBrushAppleScript.js','actionscript3 as3      @shBrushAS3.js','bash shell             @shBrushBash.js','coldfusion cf          @shBrushColdFusion.js','cpp c                  @shBrushCpp.js','c# c-sharp csharp      @shBrushCSharp.js','css                    @shBrushCss.js','delphi pascal          @shBrushDelphi.js','diff patch pas         @shBrushDiff.js','erl erlang             @shBrushErlang.js','groovy                 @shBrushGroovy.js','java                   @shBrushJava.js','jfx javafx             @shBrushJavaFX.js','js jscript javascript  @shBrushJScript.js','perl pl                @shBrushPerl.js','php                    @shBrushPhp.js','text plain             @shBrushPlain.js','ps powershell         @shBrushPowerShell.js','py python              @shBrushPython.js','ruby rails ror rb      @shBrushRuby.js','sass scss              @shBrushSass.js','scala                  @shBrushScala.js','sql                    @shBrushSql.js','vb vbnet               @shBrushVb.js','xml xhtml xslt html    @shBrushXml.js','yaml yaml              @shBrushYaml.js'));SyntaxHighlighter.defaults['toolbar']=false;SyntaxHighlighter.all();