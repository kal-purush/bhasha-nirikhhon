import { Component, OnInit } from '@angular/core';
import { FormBuilder, Validators } from '@angular/forms';
import { Router } from '@angular/router';

import { BlogService } from 'src/app/blog/blog.service';
import { BlogPost } from 'src/app/blog/models/blog-post.model';
import { CATEGORIES } from 'src/app/shared/constants';

@Component({
    selector: 'app-update-blog-post',
    templateUrl: '../new-blog-post/new-blog-post.component.html',
    styleUrls: ['../new-blog-post/new-blog-post.component.scss']
})
export class UpdateBlogPostComponent implements OnInit {
    pageTitle = 'Update Blog Post';
    buttonContent = 'Update';
    mode = 'edit';
    categories = CATEGORIES;
    titleEng: string;
    blogPostForm = this.fb.group({
        titleEng: ['', [Validators.required]],
        titleKor: ['', [Validators.required]],
        date: ['', [Validators.required, Validators.pattern('^\\d\\d\\d\\d\-\\d\\d\-\\d\\d$')]],
        category: ['', [Validators.required]],
        keywordsEng: [''],
        keywordsKor: [''],
        contentKor: [''],
        contentEng: ['']
    });

    constructor(
        private fb: FormBuilder,
        private blogService: BlogService,
        private router: Router
    ) {
        const url = this.router.url;
        const urlComponent = url.split('/');
        this.titleEng = decodeURI(urlComponent[urlComponent.length - 1]);
    }

    ngOnInit(): void {
        this.blogService.getBlogPostByTitle(this.titleEng).subscribe(data => {
            if (data.length !== 1) {
                alert('The page has an error');
                this.router.navigate(['/blog']);
            }
            this.blogPostForm.patchValue(data[0]);
            console.log(data[0])
        });
    }

    changeMode(mode: string) {
        this.mode = mode;
    }

    uploadBlogPost() {
        console.log(this.blogPostForm.value);
    }

}