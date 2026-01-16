import Vue from 'vue'

import {
   Bar
} from 'vue-chartjs'

import store from 'store'

import skills from './skills.js'

const comp = Bar.extend({
   mounted() {
      const project = store.state.session.project
      console.log('The project is %o', project)
      const sprint = project.sprints[store.state.session.sprintIndex]
      const members = [] // @TODO Not correct shape yet, no days: project.members
      members.push(project.owner)
      const now = new Date()
      const fortnightAway = new Date(+new Date() + 12096e5)
      const results = skills.skillBalance(members, now, fortnightAway, sprint)
      const allSkills = skills.toList(members, sprint)

      const hours = results.failed.map(ts => ts.hours)
      this.renderChart({
         labels: allSkills,
         datasets: [{
            label: 'Balance',
            backgroundColor: '#f87979',
            data: [hours, hours]
         }]
      })
   }
})

Vue.component('balanceChart', comp)
export default comp