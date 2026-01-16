import vue from 'vue' // var vue = require('vue')
import vuex from 'vuex'
import axios from 'axios'

let api = axios.create({
    baseURL: 'https://bcw-gregslist.herokuapp.com/api/', //here is the retrieval point
    timeout: 3000,
    // withCredentials: true
})

vue.use(vuex)


//above is standard
export default new vuex.Store({
    state:{
          posts:[
              {
                  title: "Best Story ever",
                  body: "Kick ass story",
                  imgUrl: "https://placehold.it/200x200",
                  votes: 0,
                  userId: 0,

              }
          ],
          users: [{
              name:'',
              userId: 0
          }], 
          user:[]
      },
    mutations: {
        setPosts(state, posts){
            state.posts = posts
        },  
        setUser(state, user)  {
            state.user = user
        }            
    },
    actions: {
        getPosts({dispatch, commit}){
            api.get('posts').then(res=>{
                console.log(res)
                commit('setPosts', res.data)
                
            })
        },
        addUser({ dispatch, commit }, user) {
            // api.post('users', {"userName": user}).then(res => {
            //     dispatch('getPosts')
            // })
            console.log(user.userName)
           commit('setUser',user.userName)
        },
        // checkUser({dispatch,commit},newUser){
        //     api.get('users',newUser).then(res=>{
        //         for (let i = 0; i < res.data.length; i++) {
        //             const element = res[i];
        //             if (newUser.name==res.data[i].name){
        //                 commit('setUser', res.data[i])
        //                 return true
        //             }
        //             dispatch('addUser', newUser.name)
        //             return false
        //         }
        //     })
        // }
    }





})