import utils from 'mk-utils'
import ReactDOM from 'react-dom'

export default class action {
    constructor(option) {
        this.metaAction = option.metaAction
    }

    onInit = ({ component, injections }) => {
        this.component = component
        this.injections = injections
    }

    fieldChange = async (fieldPath, value, checkFn) => {
        await this.check([{ path: fieldPath, value, checkFn }])
    }

    /*
    [{
            path: 'data.form.name', value: 'name', checkFn: ()=>{}
        }, {
            path: 'data.form.code', value: 'code', checkFn: ()=>{}
        }]
    */
    check = async (option, needSaveFieldValue) => {
        if (!option || !utils._.isArray(option))
            return

        var checkResults = []

        for (var o of option) {
            let checkResult

            if (o.checkFn) {
                checkResult = await o.checkFn({ path: o.path, value: o.value })
            }

            if (checkResult) {
                checkResults.push(checkResult)
            }
        }

        var hasError = false, json
        if (needSaveFieldValue) {
            json = option.map(o => ({ [o.path]: o.value }))
        }

        if (checkResults.length > 0) {
            checkResults.forEach(o => {
                json[o.errorPath] = o.message
            })
            if(o.message)
                hasError = true
        }

        if(json){
            this.metaAction.sfs(json)
        }
    }

}