import { ChangeDetectionStrategy, Component } from '@angular/core';
import { Tag } from 'src/app/core/models/tag/tag';
import { trackByValue } from 'src/app/core/utils/angular/track-by';
import { TagService } from 'src/app/services/tag.service';

@Component({
  selector: 'app-tags',
  templateUrl: './tags.component.html',
  styleUrls: ['./tags.component.css'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TagsComponent {

  protected readonly tags$ = this.tagService.getTags();

  protected readonly trackByTag = trackByValue<Tag>();

  public constructor(
    private readonly tagService: TagService,
  ) {}
}