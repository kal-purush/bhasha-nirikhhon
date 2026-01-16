import {NewITest} from "../store/tests-store"

const testAllConditions = (el: any, conditions: {}) => {
    return Object.entries(conditions).reduce((acc, cond) => {
        return acc && (cond[1] !== '' ? el[cond[0]].name === cond[1] : true)
    }, true)
}
export const setItemsList = (
    conditions: {},
    output: string,
    testsArray: NewITest[]
) => {
    return testsArray.reduce((acc, el: NewITest) => {
        //send el and conditions to testAllConditions function
        if (testAllConditions(el, conditions)) {
            acc = [...acc, { id: el[output]._id, value: el[output].name }]
        }

        //returns unique values
        return acc.filter(
            (a: any, i: any, self: any) =>
                self.findIndex((s: any) => a.value === s.value) === i
        )
    }, [] as any)
}