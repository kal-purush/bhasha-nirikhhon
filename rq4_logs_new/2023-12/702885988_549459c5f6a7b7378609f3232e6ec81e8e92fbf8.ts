import { Component, OnInit } from "@angular/core";
import { IBlog } from "src/app/admin/blog/service/blog.module";
import { BlogService } from "src/app/admin/blog/service/blog.service";
// import { NavbarComponent } from "src/libs/component/layout/page/navbar/navbar.component";
@Component({
  selector: 'app-blog',
  templateUrl:'./bloghome-component.html',
  styleUrls:['./bloghome-component.css','./Blogs.css'],
})


export class BlogHomeComponent implements OnInit{
  blogs: IBlog[] = [];
constructor(private blogService: BlogService ){

}

  ngOnInit(): void {
   this.getAll();
  }


getAll(){
  this.blogService.getBlog().then(blog =>{
      if(blog && blog.content){
        this.blogs =blog.content;
        console.log(blog)
      }
  })

}



}