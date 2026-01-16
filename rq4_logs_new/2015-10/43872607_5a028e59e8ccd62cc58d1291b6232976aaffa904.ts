import { Component, NgFor } from 'angular2/angular2'

@Component({
  selector: 'co-tags',
  templateUrl: 'app/components/formTools//coTags/coTags-component.html',
  inputs: ['tags'],
  directives: [NgFor]
})
export class CoTagsComponent {

  tags

  onInit () {
    console.log(this.tags)
  }

  typing ($event) {
    if ($event.keyCode === 13) {
      this.addTag($event.target.value)
    }
  }

  addTag (newTag) {
    let exists = this.tags.some((tag) => {
      return tag === newTag
    })
    if (!exists) {
      this.tags.push(newTag)
    } else {
      console.log('already exists')
    }
  }

  removeTag (tagToRemove) {
    this.tags.splice(this.tags.indexOf(tagToRemove), 1)
  }
}