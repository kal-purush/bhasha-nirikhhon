import Vuex from 'vuex'
import Cookie from 'js-cookie'

const createStore = () => {
    return new Vuex.Store({
        state:{
            loadedPosts:[]
        },
        mutations:{
            addPost(state,post){
                state.loadedPosts.push(post)
            },
            setPosts(state,posts){

                state.loadedPosts = posts
                console.log(state.loadedPosts)
            }
        },
        actions:{
            nuxtServerInit(vuexContext,context){
                return context.app.$axios
                        .$get(`${process.env.firebaseURL}/posts.json`)
                        .then(data=>{
                            const postsArray = []
                            for(const key in data){
                                postsArray.push({...data[key],id:key})
                            }
                            vuexContext.commit('setPosts',postsArray)
                        })
            },
            addPost(vuexContext,createdPost){
                return this.$axios
                        .$post(`${process.env.firebaseURL}/posts.json`,createdPost)
                        .then(data=>{
                        vuexContext.commit('addPost',{...createdPost,id:data.name})
                        // this.clear()
                        // alert('post successfully created');
                        this.$router.push('/')
                        })
                        .catch(e =>{
                        console.log(e)
                        })
            }
        },
        getters:{
            loadedPosts(state){
                return state.loadedPosts
            }
        }
    })
}

export default createStore