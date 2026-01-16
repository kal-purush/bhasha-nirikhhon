import {Component, bootstrap,FORM_DIRECTIVES,CORE_DIRECTIVES} from 'angular2/angular2';
@Component(
{
	selector:'checklistbox',
	template:`
		<div class="checkbox" *ng-for="#item of list">
			<input type="checkbox"  (change)="selectedItem(item)" />
			<label *ng-if="display==null || display==undefined || display==''">{{item}}</label>
			<label *ng-if="display!=null && display!=undefined && display!=''">{{item[display]}}<label>
		</div>
		<fieldset>

				<legend>SelectedValue</legend>
				<label *ng-for="#item of models">
						<div *ng-if="display==null || display==undefined || display==''">

						{{item}},
						</div>
						<div *ng-if="display!=null && display!=undefined && display!=''">
						{{item[display]}},
						</div>
				</label>
				
		</fieldset>
	`,
	properties:['list:cb-list','display:cb-display','value:cb-value','models:cb-model'],
	directives:[FORM_DIRECTIVES,CORE_DIRECTIVES]

})
export class CheckListBox
{
	list=[];
	models:[];
	display:string;
	value:string;

	constructure()
	{

		for(var index=0;index<this.list.length;index++)
		{
			var item = this.list[index];
			var hasValue = this.findModel(item);
			item.selected  = hasValue;
		}
	}
	findModel(item)
	{
			
			for(var index=0;index<this.models.length;index++)
			{
					var m_item = this.models[index];
					if(this.value==null || this.value==undefined || this.value=="")
					{
						if(item == m_item)
						{
							return true;
						}
					}else
					{
						if(item[this.value]==m_item[this.value])
						{
							return true;
						}
					}
			}
			return false;
	}
	findIndex(item)
	{
		for(var index=0;index<this.models.length;index++)
			{
					var m_item = this.models[index];
					if(this.value==null || this.value==undefined || this.value=="")
					{
						
						if(item == m_item)
						{
							return index;
						}
					}else
					{
						if(item[this.value]==m_item[this.value])
						{
							return index;
						}
					}
			}
			return -1;
	}
	selectedItem(item)
	{
				
			
				var hasValue = this.findIndex(item);
				if(hasValue==-1)
				{
						this.models.push(item);
				}else
				{
						this.models.splice(hasValue,1);
				}
			
	}


}
