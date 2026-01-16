import { Directive, HostListener } from '@angular/core'
import { Router } from '@angular/router'
import { MemosService } from './memos.service'
import { Memo } from '@app/shared'

@Directive({
  selector: '[manageMemo]'
})
export class ManageMemoDirective {
  constructor(private router: Router, private memosService: MemosService) {}

  @HostListener('edit', ['$event'])
  editMemo(memo: Memo) {
    this.router.navigate([{ outlets: { modal: ['update', memo.id] } }], { skipLocationChange: true })
  }

  @HostListener('delete', ['$event'])
  deleteMemo(memo: Memo) {
    this.memosService.deleteMemo(memo.id).subscribe()
  }
}