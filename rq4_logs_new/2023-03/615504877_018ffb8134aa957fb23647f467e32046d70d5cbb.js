const { createApp } = Vue 
const url = 'https://mindhub-ab35.onrender.com/api/amazing-events'

const app = createApp( {
    data(){
        return {
            events: [],
            categories: [],
            checks: [],
            text:"",
            filtro: []
        }
    },
    created(){
        this.fetchApi()
        this.getCategories()
        this.filterData()
        
    },
    methods: {
        async fetchApi(){
        try{
            let response = await fetch(url)
            response = await response.json()


            for(let ev of response.events){
                if(ev.date < response.currentDate){
                    this.events.push(ev)
                }
            }

            this.filtro = this.events
           
        }catch(error){
            console.log(error)
        }
        },
        async getCategories(){
            try{
                let response = await fetch(url)
                response = await response.json()
                response = response.events.map(each => each.category)
                response = [...new Set(response)]
                console.log(response)
                this.categories = response
        }catch(error){
            console.log(error)
        }

    },
        filterData(){
            
            this.filtro = this.events.filter(each =>{
                return(each.name.toLowerCase().includes(this.text))&&(this.checks.length === 0 || this.checks.includes(each.category))
            })
            console.log(this.filtro)
        }
    }

})
app.mount('#app')