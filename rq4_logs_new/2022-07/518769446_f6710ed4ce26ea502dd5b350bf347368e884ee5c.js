var cur = document.querySelector(".container-tools");
function openPage(c) {
  var next = document.querySelector(c);
  if (cur != next) {
    cur.classList.toggle("container-active");
    next.classList.toggle("container-active");
    cur = next;
  }
}
function validateVideoLink(url) {
  if (url != undefined || url != "") {
    var regExp =
      /^.*(youtu.be\/|v\/|u\/\w\/|embed\/|watch\?v=|\&v=|\?v=)([^#\&\?]*).*/;
    var match = url.match(regExp);
    if (match && match[2].length == 11) {
      return true;
    } else return false;
  }
}
async function ShowThumbnail(p) {
  let val = p.querySelector(".url-input").value;

  let done = false;
  if (validateVideoLink(val)) {
    let videoId = val.split("?v=")[1].split("&")[0];
    let r = await fetch(`/thumbnail?videoId=${videoId}`);
    if (r.status == 200 && !done) {
      done = true;
      r = await r.json();
      console.log(r);
      let thumb = p.querySelector(".thumbnail");
      thumb.querySelector("img").setAttribute("src", r.thumbnailUrl);
      thumb.querySelector("p").textContent = r.title;
      thumb.classList.add("thumbnail-active");
    } else {
      console.log("error");
    }
  }
}
async function showCommentSearchResult(p) {
  let tagOptions = p.querySelector(".tag-options");
  let keyword = tagOptions.querySelector("#tag-option-keyword");
  let searchBy = "user";
  if (keyword.checked) searchBy = "keyword";
  let query = p.querySelector(".tag-input").value;
  let val = p.querySelector(".url-input").value;
  let html = "";
  let result = p.querySelector(".result-comments");
  if (validateVideoLink(val)) {
    let videoId = val.split("?v=")[1].split("&")[0];
    let res = await fetch(
      `/comment-search?videoId=${videoId}&searchBy=${searchBy}&query=${query}`
    );
    if (res.status == 200) {
      res = await res.json();

      for (let i = 0; i < res.length && i < 50; i++) {
        let c = res[i];
        html += `
                  <a href = "${c.link}" target = "_blank">
                  <div class="comment">
                      <div class="comment-user">
                          <img src="${c.userImg} ">
                               <p> ${c.user} </p>
                               <p> <i class ="fas fa-thumbs-up"></i> ${c.likeCount} </p>    
                          </div>
                          <div class="comment-text">
                              ${c.text}
                          </div>
                          </div>
                  </a>
                          `;
      }
      if (res.length == 0)
        html = `<div class="comment"> <div class="comment-text"> No Comments Found !</div> </div>`;
    } else {
      html = `<div class="comment"> <div class="comment-text"> Error : Bad Request !</div> </div>`;
    }
  } else {
    html = `<div class="comment"> <div class="comment-text"> Wrong Url !</div> </div>`;
  }
  result.innerHTML = html;
  result.classList.add("result-comments-active");
}
async function showRandomCommentResult(p) {
  let keyword = p.querySelector(".tag-input").value;
  let val = p.querySelector(".url-input").value;
  let result = p.querySelector(".result-comments");
  let html = "";
  if (validateVideoLink(val)) {
    let videoId = val.split("?v=")[1].split("&")[0];
    let res = await fetch(
      `/random-picker?videoId=${videoId}&keyword=${keyword}`
    );
    if (res.status == 200) {
      try {
        c = await res.json();
        console.log(res);
        html = `
                      <a href = "${c.link}" target = "_blank">
                      <div class="comment">
                          <div class="comment-user">
                              <img src="${c.userImg} ">
                                  <p> ${c.user} </p>
                                  <p> <i class ="fas fa-thumbs-up"></i> ${c.likeCount} </p>    
                              </div>
                              <div class="comment-text">
                                  ${c.text}
                              </div>
                              </div>
                      </a>
                              `;
      } catch (e) {
        html = `<div class="comment"> <div class="comment-text"> No Comments Found !</div> </div>`;
      }
    } else {
      html = `<div class="comment"> <div class="comment-text"> Error : Bad Request !</div> </div>`;
    }
  } else {
    html = `<div class="comment"> <div class="comment-text"> Wrong Url !</div> </div>`;
  }
  result.innerHTML = html;
  result.classList.add("result-comments-active");
}
async function showCaption(p) {
  let val = p.querySelector(".url-input").value;
  let result = p.querySelector(".result-comments");
  let html = "";
  if (validateVideoLink(val)) {
    let videoId = val.split("?v=")[1].split("&")[0];
    let res = await fetch(
      `/caption?videoId=${videoId}`
    );
    if (res.status == 200) {
        res = await res.json();

        for (let i = 0; i < res.length && i < 50; i++) {
          let c = res[i];
          html += `
                    <div class="comment">
                            <div class="comment-text">
                                ${c.language}
                            </div>
                            </div>
 
                            `;
        }
        if (res.length == 0)
          html = `<div class="comment"> <div class="comment-text"> No Captions Found !</div> </div>`;
      }else {
      html = `<div class="comment"> <div class="comment-text"> Error : Bad Request !</div> </div>`;
    }
  } else {
    html = `<div class="comment"> <div class="comment-text"> Wrong Url !</div> </div>`;
  }
  result.innerHTML = html;
  result.classList.add("result-comments-active");
}