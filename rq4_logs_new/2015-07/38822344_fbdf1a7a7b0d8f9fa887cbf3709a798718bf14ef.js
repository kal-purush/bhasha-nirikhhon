/*
 * Copyright 2015 FUJITSU LIMITED
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

define(['angular'], function (angular) {
  'use strict';
  var module = angular.module('grafana.services');

  module.factory('dashboardStorage', function($q, NamespaceLocalStorage) {

    var store = new NamespaceLocalStorage('dashboards');

    function prepareQuery(query) {
      if(!query){
        query = '';
      }
      var tags = query;
      var title = query;
      var tagsOnly = query.indexOf('tags!:') === 0;
      if(tagsOnly){
        tags = query.substring(6, query.length).replace(/^\s|\s$/,'');
        title = '';
      }else{
        var titleOnly = query.indexOf('title:') === 0;
        if (titleOnly) {
          title = query.substring(6, query.length).replace(/^\s|\s$/,'');
          tags = '';
        }
      }
      return {
        title: title.toLowerCase(),
        tags: tags.toLowerCase()
      };
    }

    return {
      get: function(id) {
        var deferred = $q.defer();
        try {
          deferred.resolve(store.get(id));
        }catch(e) {
          deferred.reject(e);
        }
        return deferred.promise;
      },
      save: function(model) {
        var deferred = $q.defer();
        try{
          var result = model.id ? store.update(model) : store.insert(model);
          deferred.resolve({
            title: result.title,
            url: '/dashboard/db/' + result.id
          });
        }catch(e) {
          deferred.reject(e);
        }
        return deferred.promise;
      },
      search: function(query) {
        var result = prepareQuery(query);
        var title = result.title;
        var tags = result.tags;

        var hits = {
          dashboards: [],
          tags: [],
          tagsOnly: false
        };
        var deferred = $q.defer();
        try {
          var results = title || tags ? store.search(title, tags) : store.all();
          hits.dashboards = results.map(function(item) {
            return {
              id: item.id,
              title: item.title,
              tags: item.tags
            };
          });
          deferred.resolve(hits);
        }catch(e) {
          deferred.reject(e);
        }
        return deferred.promise;

      },
      remove:function(id) {
        var deferred = $q.defer();
        try {
          var result = store.get(id);
          store.remove(id);
          deferred.resolve(result.title);
        }catch(e) {
          deferred.reject(e);
        }
        return deferred.promise;
      }
    };
  });

});