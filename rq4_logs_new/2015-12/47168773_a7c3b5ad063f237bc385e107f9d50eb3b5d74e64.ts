/// <reference path="../../dto/task.dto.ts" />

// import {TaskDTO} from 'dto/task.dto';

Template['task'].events({
	'click .item': function(event: any): any {
		var $this = $(event.target);
		$this.addClass("crossed");
	},
	'submit .form': function(event:any): any {
		event.preventDefault();
		
		Meteor.call("TaskService.addTask", {title: event.target['title'].value}, function(err: Meteor.Error, res: string) {
			if (err != null) {
				console.log(err);
			} else {
				console.log(res);
				
				if (res != null) {
					alert("Added");
				}
			}
		});
	}
});