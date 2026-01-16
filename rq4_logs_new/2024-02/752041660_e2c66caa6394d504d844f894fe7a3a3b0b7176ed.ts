import { Component, Input } from '@angular/core'

@Component({
    selector: 'app-color-picker',
    standalone: true,
    imports: [],
    templateUrl: './colorpicker.component.html',
})
export class ColorPickerComponent {
    @Input() public startColor = ""
    @Input() public onColorChange: ((args: any) => void) = () => { console.log("Unimplemented") }

    ngOnInit() {
        (document.getElementById('colorInput') as HTMLInputElement).value = this.startColor
    }
}