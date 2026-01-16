// 路由
//可以用来做单页，Router.init();
define(function(require, exports, module) {

  'use strict';

  // 路由
  var Router = new Object;

  // 防止ie8设置location.hash时触发Router.parse
  Router.LAST_CALL_FIRED_BY_JAVASCRIPT = false;
  // 可访问的页面，访问其他路径丢404
  Router.urls = ['index', 'video', 'coin', 'follow', 'fans', 'setting', 'favlist', 'fav', 'bangumi', 'subs', 
                 'qz-index', 'qz-threads', 'qz-replys', 'qz-fav'];

  // 获取访问页面
  Router.getUrl = function() {
    return document.location.hash.replace('#!/', '').split('/')[0];
  }

  // 获取参数
  Router.getParams = function() {
    return document.location.hash.replace('#!/', '').split('/').slice(1);
  }

  // 修改url
  Router.setUrl = function(url, obj) {
    if( url[0] != '/' ) url = '/' + url;
    if( window.history && history.pushState ) {
      history.pushState(obj ? obj : {}, null, '#!' + url);
    } else if( document.location.hash != '#!' + url ) {
      Router.LAST_CALL_FIRED_BY_JAVASCRIPT = true;
      document.location.hash = '!' + url;
    }
  }

  // 替换url，这个操作会删除上一步的history，适用于404等有重定向的页面
  Router.replaceUrl = function(url, obj) {
    if( url[0] != '/' ) url = '/' + url;
    if( window.history && history.replaceState ) {
      history.replaceState(obj ? obj : {}, null, '#!' + url);
    } else if( document.location.hash != '#!' + url ) {
      Router.LAST_CALL_FIRED_BY_JAVASCRIPT = true;
      document.location.hash = '!' + url;
    }
  }

  // 修改参数
  Router.setParams = function() {
    var url = Router.getUrl();
    Router.setUrl(url + '/' + $.makeArray(arguments).join('/'));
  }

  Router.load = function(url) {
    var body = $('.s-body').attr('id', '').empty().addClass('loading');
    // 读取url指定的页面
    var html = sessionStorage.getItem('PAGE_CACHE_' + url);
    if( !html || IS_DEV ) {
      $.get(ROOT_PATH + '/html/' + url + '.html?_=' + new Date().getTime(), function(_html) {
        // cache
        sessionStorage.setItem('PAGE_CACHE_' + url, _html);
        // 替换mid
        html = _html.replace(/{{\s*space\.mid\s*}}/g, _bili_space_info.mid);
        body.attr('id', 'page-' + url).html(html).removeClass('loading');
      });
    } else {
      // 替换mid
      html = html.replace(/{{\s*space\.mid\s*}}/g, _bili_space_info.mid);
      body.attr('id', 'page-' + url).html(html).removeClass('loading');
    }
  }

  // 解析hash
  Router.parse = function() {
    if( Router.LAST_CALL_FIRED_BY_JAVASCRIPT ) {
      Router.LAST_CALL_FIRED_BY_JAVASCRIPT = false;
      return;
    }
    var url = Router.getUrl();
    if( url == '' ) {
      // 不带mid进来时自动给url加上Mid
      var indexUrl = '/' + _bili_space_mid + '/#!/index';
      if( window.history && history.replaceState ) {
        history.replaceState({}, null, indexUrl);
        Router.load('index');
      } else {
        window.location.href = indexUrl;
      }
    } else if( url == 'setting') { 
      if( _bili_is_login && _bili_is_my_space ) {
        Router.load('setting');
      } else {
        Router.load('404');
      }
    } else if( Router.urls.indexOf(url) > -1 ) {
      Router.load(url);
    } else {
      Router.load('404');
      Router.replaceUrl('404');
    }
  }

  // 初始化路由 
  Router.init = function() {
    $(window).on('hashchange', Router.parse);
    Router.parse();
  }

  module.exports = Router;
});