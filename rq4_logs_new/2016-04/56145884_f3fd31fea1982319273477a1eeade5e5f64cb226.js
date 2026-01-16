(function ()
{
    'use strict';

    angular.module('utcApp').controller('TaskDetailCtrl', function ($scope, $routeParams, $location, ConfirmAction, TaskDAO)
    {
        var ctrl = this;

        this.mode = 'display';
        this.isCreating = false;
        this.task = {};
        this.tags = {};


        this.searchTags = function (queryObj) {
            TaskDAO.queryTags(queryObj.term).then(function (results) {
                ctrl.tags = results;
                var data = { results: ctrl.tags};
                queryObj.callback(data);
            });

        };
        this.tagsTypeaheadConfig = {
            query:  function (query) {
                ctrl.searchTags(query);
            },
            multiple: true,
            minimumInputLength: 1,
            maximumSelectionSize: 10,
            createSearchChoice: function (term) {
                return { id: term, text: term };
            }

        };

        this.init = function ()
        {
            if ($routeParams.id === 'create') {
                ctrl.task = {};
                ctrl.mode = 'edit';
                ctrl.isCreating = true;
            } else {
                TaskDAO.query({a: $routeParams.id}).then(function (task)
                {
                    ctrl.isCreating = false;
                    ctrl.task = task;
                    ctrl.mode = 'display';
                });
            }
        };

        this.toggleMode = function ()
        {
            if (this.mode === 'display') {
                this.mode = 'edit';
            } else if (this.mode === 'edit') {
                this.mode = 'display';
            }
            return this.mode;
        };

        this.save = function ()
        {
            if (this.isCreating) { // create new task
                TaskDAO.save(this.task);
                $location.path('/tasks');

            } else { // update existing task
                TaskDAO.save(this.task);
                this.mode = 'display';
            }
        };

        this.discard = function ()
        {
            if (this.isCreating) {
                ConfirmAction.open('Discard', 'Are you sure?').result.then(function ()
                {
                    ctrl.task = {};
                    $location.path('/tasks');
                });
            } else {
                if (this.mode === 'edit') {
                    this.mode = 'display';

                } else {
                    $location.path('/tasks');
                }
            }
        };

        this.getBranches = function (query) 
        {
            if (ctrl.task.repositoryUrl) {
                return TaskDAO.queryBranches(ctrl.task.repositoryUrl, query);
            }
        };

        this.init();

    });


})();