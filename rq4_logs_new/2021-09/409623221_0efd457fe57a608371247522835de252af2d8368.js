const Brainly = require("brainly-scraper-v2")
const brain = new Brainly("ph")
const country = 'ph'

const search = async (query) => {
    try {
        function removeTags(str) {
            if ((str === null) || (str === "")) return false
            else str = str.toString()
            return str.replace(/(<([^>]+)>)/ig, '')
        }

        function search(q) {
            return brain.search(country, q)
        }

        if (question === "" || question ===  null || question === undefined) {
            return "Not A Valid Question"
        } else {
            const res = await search(query)
        }
    } catch (err) {
        return `An error occurred:\n${err}`
    }
}