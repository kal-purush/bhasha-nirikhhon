import { ChangeDetectionStrategy, Component } from '@angular/core';

@Component({
  selector: 'skip-link',
  templateUrl: './skip-link.component.html',
  standalone: true,
  styleUrls: ['./skip-link.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SkipLinkComponent {
  public skipToTargetId(event: Event, targetId: string): void {
    event.stopPropagation();
    event.preventDefault();

    const elementToScrool = document.getElementById(targetId);
    elementToScrool?.scrollIntoView({
      behavior: 'smooth',
    });

    elementToScrool?.focus();
  }
}