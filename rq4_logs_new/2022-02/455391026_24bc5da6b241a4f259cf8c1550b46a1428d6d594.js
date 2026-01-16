let blogs = []

function addBlog(event) {
    
    event.preventDefault()
    
    let title = document.getElementById ('input-blog-title').value
    let content= document.getElementById ('input-blog-content').value
    let image = document.getElementById ('input-blog-image')
    image= URL.createObjectURL(image.files[0])

    let blog = {
        author: `Fatih`,
        title,
        content,
        image,
        postedAt: new Date(),
    }

    blogs.push(blog)
    renderBlog()
}

function renderBlog(){
let lengthData = blogs.length

let blogContainer = document.getElementById ("contents")

blogContainer.innerHTML
for (let i = 0; i < lengthData; i++) {
    if(i+1 == lengthData)
    blogContainer.innerHTML +=`
    <div class="blog-list-item">
    <div class="blog-image">
      <img src="${blogs[i].image}" alt="" />
    </div>
    <div class="blog-content">
      <div class="btn-group">
        <button class="btn-edit">Edit Post</button>
        <button class="btn-post">Post Blog</button>
      </div>
      <h1>
        <a href="blog-detail.html" target="_blank"
          >${blogs[i].title}</a
        >
      </h1>
      <div class="detail-blog-content">
        ${blogs[i].postedAt} | ${blogs[i].author}
      </div>
      <p>
        ${blogs[i].content}
      </p>
    </div>
  </div>`


}

function getDistanceTime(time) {
  const distance = new Date() - new Date(time)

  // convert to day
  const miliseconds = 1000
  const secondInMinute = 60
  const minutesInHour = 60
  const secondsInHour = secondInMinute * minutesInHour
  const hoursInDay = 23

  let dayDistance = distance / (miliseconds * secondsInHour * hoursInDay)

  if (dayDistance >= 1) {
    const time = Math.floor(dayDistance) + ' a day ago'
    console.log("time " + time);
    return time
  } else {
    // Convert to hour
    let hourDistance = Math.floor(distance / (miliseconds * secondsInHour))
    // hourDistance = 0.1
    if (hourDistance > 0) {
      return hourDistance + ' hour ago'
    } else {
      // convert to minute
      const minuteDistance = Math.floor(distance / (miliseconds * secondInMinute))
      return minuteDistance + ' minute ago'
    }
  }

}

setInterval(function () {
  renderBlog()
}, 2000)


}

function firstContent() {
    return`
    <div class="blog-list-item">
    <div class="blog-image">
      <img src="assets/blog-img.png" alt="" />
    </div>
    <div class="blog-content">
      <div class="btn-group">
        <button class="btn-edit">Edit Post</button>
        <button class="btn-post">Post Blog</button>
      </div>
      <h1>
        <a href="blog-detail.html" target="_blank"
          >Pasar Coding di Indonesia Dinilai Masih Menjanjikan</a
        >
      </h1>
      <div class="detail-blog-content">
        12 Jul 2021 22:30 WIB | Ichsan Emrald Alamsyah
      </div>
      <p>
        Ketimpangan sumber daya manusia (SDM) di sektor digital masih
        menjadi isu yang belum terpecahkan. Berdasarkan penelitian
        ManpowerGroup, ketimpangan SDM global, termasuk Indonesia,
        meningkat dua kali lipat dalam satu dekade terakhir. Lorem ipsum,
        dolor sit amet consectetur adipisicing elit. Quam, molestiae
        numquam! Deleniti maiores expedita eaque deserunt quaerat! Dicta,
        eligendi debitis?
      </p>
    </div>
  </div>`
    
}