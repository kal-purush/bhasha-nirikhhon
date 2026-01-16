    // Method to send the form data to
    let createPost = function() {
        let newPostForm = $("#new-postform");

        newPostForm.submit((e) => {
            e.preventDefault();

            $.ajax({
                type: 'post',
                url: '/posts/create',
                data: newPostForm.serialize(),
                success: function (data) {
                    let newPost = newPostDom(data.data.post);
                    $('#posts-container>ul').prepend(newPost);
                }, error: function (error) {
                    console.log(error.responseText);
                }
            })
        })
    }

    //Method to create post

    let newPostDom = function (post) {

        return(`
        <li id="post-${post._id}" class="post-item-container">
        <div class="post-item-header">
          <div class="user-info">
            <div class="user-image">
              <i class="far fa-user"></i>
            </div>
            <div class="post-header-details">
              <p class="user-name">
                ${ post.user.name }
              </p>
            </div>

            <div class="post-remove">
              <a href="/posts/destroy/${post._id}" class="delete-post-button">
                <i class="far fa-times-circle"></i>
              </a>
            </div>

          </div>
        </div>

        <!-- post details -->

        <div class="post-details">
          <h2>
            ${ post.content }
          </h2>
          <div class="post-item-bottom">
            <div class="post-reactions">
              <a href="" class="likes"><i class="fas fa-heart like"></i> 22 Likes</a>
              <a href="" class="comments"
                ><i class="fas fa-comment comment"></i> 15 Comments</a
              >
            </div>
          </div>
        </div>

        <!-- Comment section -->

        <div class="post-comment">

          <form action="/comments/create" method="POST" class="comment-form">
            <input
              type="text"
              name="content"
              placeholder="Add a comment..."
              required
              class="comment-input"
            />
            <input type="hidden" name="post" value="${post._id }" />
            <button type="submit" class="submit-com-btn">
              <i class="fas fa-comment"></i>
            </button>
          </form>


          <!--Comments list  -->
          <div class="post-comments-list">
            <ul id="post-comments-${post._id}">

            </ul>
          </div>
        </div>
      </li>

        `)
    }

    let addPost = $('#post-btn');

    addPost.on('click',createPost());

