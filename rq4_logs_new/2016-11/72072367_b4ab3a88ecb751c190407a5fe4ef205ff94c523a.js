var postId = getURLParameter("postId")
var postName = getURLParameter("postName")
var boardName = getURLParameter("boardName")
var boardId = getURLParameter("boardId")

$(document).ready(function () {

    Parse.initialize("8f5d3fea-573c-4a82-8f9e-5e2873cc6d52");
    Parse.serverURL = 'http://responsum.herokuapp.com/parse';

    //setup nav bar

    $("#navBar").append("<li role='presentation' class=''><a href='board.html?boardId=" + boardId + "&boardName="+boardName+"'>"+boardName+"</a></li><li role='presentation' class='nav navbar-brand icon ion-ios-arrow-right text-center'><a href=''></a></li><li role='presentation' class='active'><a href=''>"+postName+"</a></li>")

    //configure post
    configurePost(postId);

})

//for configuring our post
function configurePost(postId){
    //query the post
    var Post = Parse.Object.extend("Post")
    var postQuery = new Parse.Query(Post)
    postQuery.get(postId,{
        success:function(fetchedPost){
            //update page
            $("#postTitle").append(fetchedPost.get("title"));
            $("#postBody").append(fetchedPost.get("body"));
        },
        error:function(error){
            console.log("error querying posts: "+JSON.stringify(error))
        }
    })

}

//for parsing whatever parameters we have in the url
function getURLParameter(name) {
    return decodeURIComponent((new RegExp('[?|&]' + name + '=' + '([^&;]+?)(&|#|;|$)').exec(location.search) || [null, ''])[1].replace(/\+/g, '%20')) || null;
}