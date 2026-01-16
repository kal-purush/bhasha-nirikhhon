var ACTIVITY_UI = (function () {
	function TagManager(tagInputId, addButtonId, displayListId){
		this.tagInput= $('#' + tagInputId);
		this.addButton = $('#' + addButtonId);
		this.displayList = $('#' + displayListId);

		this.tagArray = [];

		var obj = this;
		this.addButton.click(function(){obj.addTag();});
	}

	TagManager.prototype.addTag = function(newTagName) {

			var t;
			if (newTagName == null)
				t = this.stripTag();
			else
				t = newTagName;
			//if the tag exists in the list do nothing
			for(var i=0; i < this.tagArray.length; i++){			
				console.log(i);
				if (this.tagArray[i].children()[0].innerHTML.substring(1) == t){					
					return;
				}
			}

			//otherwise create a new <li>
			//var newTag = $('<li><span class="label label-info">#' + this.stripTag() +'</span></li>');
			var newTag = $('<li><span class="label label-info">#' + t +'</span><input type="hidden" name="tags[' + this.tagArray.length + ']" value="' + t + '" /></li>');
			
			
			//store the <li> into the array and set it's click handler for removal
			this.tagArray.push(newTag);
			var obj = this;
			newTag.click(function(){				
				obj.removeTag(newTag.children()[0].innerHTML);
			})
			newTag.appendTo(this.displayList);
	};

	TagManager.prototype.removeTag = function(tagName){

		var idx = this.tagArray.map(function (jElement) {
	    return jElement.find('span').text();
		}).indexOf(tagName);
		
		var element = this.tagArray[idx];		
		element.remove();
		this.tagArray.splice(idx, 1);
	}

	TagManager.prototype.stripTag = function() {
		var tagName = this.tagInput.val().replace(/ /g,'');
		return tagName.replace('#','');	
	};

	TagManager.prototype.loadExistingTags = function(existingTagArray){
		for(var i=0; i< existingTagArray.length; i++){
			this.addTag(existingTagArray[i]);
		}
	}

	function ScoredRangeInputManager(activityTypeSelectorId, rangeDivId){
		this.rangeDiv = $('#' + rangeDivId);
		this.activityTypeSelector = $('#' + activityTypeSelectorId);
		if(this.activityTypeSelector.val() == 'scored'){
			this.showRange();
		}
		else{
			this.hideRange();
		}

		var obj = this;

		this.activityTypeSelector.change(function(){
			if(obj.activityTypeSelector.val() == 'scored'){
				obj.showRange();
			}
			else{
				obj.hideRange();
			}
		});
	}

	ScoredRangeInputManager.prototype.showRange = function(first_argument) {
		this.rangeDiv.attr('style','display:block');
	};

	ScoredRangeInputManager.prototype.hideRange = function(first_argument) {
		this.rangeDiv.attr('style', 'display:none');	
	};

	return {
		TagManager: TagManager,
		ScoredRangeInputManager: ScoredRangeInputManager
	};
})();