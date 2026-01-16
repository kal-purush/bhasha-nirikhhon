import {Component, ViewChild, AfterViewInit,ElementRef,HostListener,OnInit} from '@angular/core';
import {Router,ActivatedRoute} from "@angular/router";
import { LocalStorageService, SessionStorageService } from 'ng2-webstorage';
import { UserService } from '../providers/userservice';
import { SecureService } from '../providers/secureservice';
import { SearchPackagePipe } from '../pipes/searchpackage-pipe';
import { PackageTypePipe } from '../pipes/packagetype-pipe';
import { ModalDirective } from 'ngx-bootstrap/modal';
declare var jQuery:any;
declare var google:any;

@Component({
    selector: 'panorama-domestic',
    templateUrl: 'domestic.component.html',
    styleUrls: ['domestic.component.css']
})

export class PanoramaDomesticComponent  {

    @ViewChild('lgModal') public lgModal: ModalDirective;

    public variable:String;
    public allDoms:any=[];
    public currentTour:any={};
    public customSearch:string='';

    constructor(private router: Router,private userService: UserService, private route: ActivatedRoute,public ss:SecureService,public storage:LocalStorageService){

      //console.log(this.storage.retrieve('activeUser'));
       this.userService.getAllDomPackages().subscribe((res)=>{
          if(res.status==200){
              this.allDoms=this.userService.allDomPackages;
          }
      });
    }

    openModal(t:any){
        this.currentTour=t;
        this.lgModal.toggle();
    }

    ngDoCheck(){
        this.allDoms=this.userService.allDomPackages;
    }


    ngOnInit() {

      jQuery('.singletour').each(function(i){
						setTimeout(function(){
							jQuery('.singletour').eq(i).addClass('is-showing');
						}, 200*(i+1));
					})



			jQuery(window).scroll(function(){
				// alert(jQuery('.grouptours').offset().top);
				// alert(jQuery(window).height()*0.5);
				var wScroll = jQuery(this).scrollTop();
				if(wScroll > (jQuery('.grouptours').offset().top)-(jQuery(window).height()*0.8)){
					jQuery('.singletour').each(function(i){
						setTimeout(function(){
							jQuery('.singletour').eq(i).addClass('is-showing');
						}, 200*(i+1));
					})
				}

				if(wScroll > (jQuery('.fixedtours').offset().top)-(jQuery(window).height()*0.8)){
					jQuery('.singlefixedtour').each(function(i){
						setTimeout(function(){
							jQuery('.singlefixedtour').eq(i).addClass('is-showing');
						}, 200*(i+1));
					})
				}
			});


    }

    toShowTitle(type:any){
        let count:number=0;
        for(var i=0; i < this.allDoms.length ; i++){
          if(this.allDoms[i].packagetype==type){
            count++;
            return true;
          }
        }

        if(count==0){
          return false;
        }
    }

    printStatement(){
      var mywindow = window.open('', 'Statement', 'height=0,width=0');
        mywindow.document.write('<html><head><title>Statement</title>');
        mywindow.document.write('<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.6/css/bootstrap.min.css" integrity="sha384-1q8mTJOASx8j1Au+a5WDVnPi2lkFfwwEAa8hDDdjZlpLegxhjVME1fgjWPGmkzs7" crossorigin="anonymous" type="text/css" />');
        mywindow.document.write('<link rel="stylesheet" href="../stylesheets/print.css" type="text/css" />');
        mywindow.document.write('</head><body>');
        mywindow.document.write('<div class="modal-content modalcontent">');
        mywindow.document.write(jQuery('#tourmodal').html());
        mywindow.document.write('<div></body></html>');

        mywindow.document.close(); // necessary for IE >= 10
        mywindow.focus(); // necessary for IE >= 10

        mywindow.print();
        // mywindow.close();

        return true;

    }

    goToLogin(){
        if(this.storage.retrieve('activeUser')==null || this.storage.retrieve('activeUser')==undefined){
            this.router.navigate(['/login']);
        }else if(this.storage.retrieve('activeUser').roles['3'] !=null && this.storage.retrieve('activeUser').roles['3'] != undefined && this.storage.retrieve('activeUser').roles['3']=='administrator'){
            console.log(this.storage.retrieve('activeUser').roles['3']);
            this.router.navigate(['/admin']);
        }else if(this.storage.retrieve('activeUser').roles['2'] !=null && this.storage.retrieve('activeUser').roles['2'] != undefined && this.storage.retrieve('activeUser').roles['2']=='authenticated user' && this.storage.retrieve('activeUser').roles['3']==undefined){
          console.log(this.storage.retrieve('activeUser').roles['2']);
          this.router.navigate(['/user']);
        }

    }
}