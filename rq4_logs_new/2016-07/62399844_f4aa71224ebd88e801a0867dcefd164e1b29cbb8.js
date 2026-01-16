var Backbone = require('backbone');
var mongoose = require('mongoose');
var _ = require('underscore');
//var FormView = require('./FormView');
var EditDeptView = require('./EditDeptView');
var DepartmentsCollection = require('../collections/DepartmentsCollection');
var DepartmentsModel = require('../models/DepartmentsModel');
//var TableListView = require('./TableListView');
var DepartmentItemView = Backbone.View.extend({
    el: '<tr></tr>',

    template: _.template('\
    <td><%= departments.get("departments") %></td>\
    <td><button class="btn btn-success href="" id="editDept">Edit Department</button></td>\
    <td><button class="btn btn-danger href="" id="destroyDept">Delete Department</button></td>\
  '),
  events: {
    'click #editDept': "handleUpdate",
    'click #destroyDept': 'handleDestroyDept'
  },
  handleUpdate: function(){
    //this.model.get('');
    console.log(this.model);
    var editdeptView = new EditDeptView({
      model: this.model
   });

    editdeptView.render();
    $('#content').html(editdeptView.el);
  },
  handleDestroyDept: function(){
    //this.model.get('computer');
    console.log(this.model);
    this.model.destroy();
  },
    render: function() {
      //e.preventDefault();
      //event && event.preventDefault();
        $(this.el).html(this.template({departments: this.model}));
        return this;
    }
});
module.exports = DepartmentItemView;