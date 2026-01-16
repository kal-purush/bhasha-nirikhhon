/// <reference path="../../typings/all.ts" />

class Session {
    dir:string
    files:DirItem[]
    selected:boolean

    constructor(dir:string) {
        this.setDir(dir)
    }

    setDir(newDir:string) {
        this.dir = newDir
        this.files = fs.readdirSync(newDir).map(function (item:string) {
            return new DirItem(path.join(newDir, item))
        })
        console.log(this.files)
    }

    setSelected(newValue: boolean) {
        this.selected = newValue
    }

    html_class() {
        return this.selected_text()
    }

    private selected_text():String {
        return this.selected ? "selected" : "unselected"
    }
}
