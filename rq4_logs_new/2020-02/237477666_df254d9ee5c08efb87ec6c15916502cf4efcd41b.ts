import {ChangeDetectorRef, Component, OnInit, ViewChild} from '@angular/core';
import {ActivatedRoute, Router} from "@angular/router";
import {JobsService} from "../services/jobs.service";
import {faLandmark, faMapMarker, faHandPointRight, faMoneyBill, faFileContract, faFlagUsa, faGlobe} from "@fortawesome/free-solid-svg-icons";
import {FormBuilder, FormGroup, Validators} from "@angular/forms";
import Swal from 'sweetalert2';

const ALLOWED_EXTS = ['pdf', 'doc', 'docx'];

@Component({
  selector: 'app-job-application-page',
  templateUrl: './job-application-page.component.html',
  styleUrls: ['./job-application-page.component.css']
})
export class JobApplicationPageComponent implements OnInit {
  jobId: string = null;
  job = {
    title: '', company: '', location: '', salary_range: 0, job_type: 0, is_remote: false, visa_sponsor: false,
  };
  form: FormGroup;
  @ViewChild('lblResume', { static: true }) lblResume;
  resumeFile;

  faLandMark = faLandmark;
  faMapMarker = faMapMarker;
  faHandPointRight = faHandPointRight;
  faMoneyBill = faMoneyBill;
  faFileContract = faFileContract;
  faFlagUsa = faFlagUsa;
  faGlobe = faGlobe;


  constructor(private router: Router, private route: ActivatedRoute, private jobService: JobsService, private formBuilder: FormBuilder, private changeDetector: ChangeDetectorRef) {
    this.jobId = route.snapshot.params.job_id;
    this.jobService.getJobInfo(this.jobId).subscribe(r => {
      this.job = r['payload'];
    });
    this.form = this.formBuilder.group({
      name: ['', Validators.required],
      email: ['', Validators.required],
      resume: [null, Validators.required], // file upload
    });
  }

  ngOnInit() {
  }

  get f() {
    return this.form.controls;
  }

  onFileChange(e) {
    this.lblResume.nativeElement.innerText = this.f.resume.value.split('\\').pop();

    if(e.target.files && e.target.files.length) {
      const [file] = e.target.files;

      const ext = file.name.split('.').pop();
      if (ALLOWED_EXTS.indexOf(ext) < 0) {
        Swal.fire({
          type: 'error',
          title: 'Error',
          text: 'Invalid file type. Only allows: .doc, .docx, .pdx extensions file only'
        });
      }

      this.resumeFile = file;
    }
  }

  onSubmit() {
    if (this.form.invalid) {
      console.log('error');
      return;
    }

    this.jobService.postJobApplication(this.jobId, {
      name: this.f.name.value,
      email: this.f.email.value,
      resume: this.resumeFile,
    }).subscribe(r => {
      Swal.fire({
          title: 'Congratulation!',
          text: 'Your application for this position has been submitted successfully!',
          type: 'success',
          confirmButtonText: 'Okay'
      }).then(r => {
        this.router.navigate(['/jobs', this.jobId]);
      });
    }, err => {
      Swal.fire({
        title: 'Error!',
        text: 'Something went wrong! Please try again or contact us.',
        type: 'error',
        confirmButtonText: 'Okay'
      })
    });
  }

}