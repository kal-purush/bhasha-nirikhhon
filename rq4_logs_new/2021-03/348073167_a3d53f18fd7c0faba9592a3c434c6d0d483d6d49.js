$(function(){
	const todoList = {
		$container: $('.todo-list'),
		$input: $('input:text'),
		$btn: $('.btn-addTask'),

		taskTemplate: `<label href="#" class="list-group-item list-group-item-action d-flex align-items-center">
							<input type="checkbox" class="mr-2"> 
							<span class="flex-grow-1">Estudar HTML</span>
							<button type="button" class="btn btn-danger btn-sm">Apagar</button>
						  </label>`,

		init: function () {
			this.$input.on('keyup', (e) => {
				let task = this.$input.val();
				if (e.keyCode === 13 && task) {
					this.addTask(task);
				}
			})

			this.$btn.on('click', (e) => {
				let task = this.$input.val();
				if (task) {
					this.addTask(task);
				}
			})

			this.$container.on('change','[type="checkbox"]', e => {
				$(e.target).closest('.list-group-item').toggleClass('done');
			})

			this.$container.on('click','.btn', e => {
				this.excluirtask($(e.target).closest(".list-group-item"));
			})
		},

		addTask: function(task) {
			let $newtask = $(this.templatetask);
			$newtask.find('span').text(task);

			this.$container.append($newtask);
			this.$input.val('').focus();
		},

		excluirtask: function(taskItem) {
			// taskItem.remove();
		}
	}
	todoList.init();
});