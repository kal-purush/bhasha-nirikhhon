import { Injectable, inject } from '@angular/core';
import { ComponentStore } from '@ngrx/component-store';
import { Episode, EpisodesService, emptyEpisode } from '@rick/api';
import { Observable, switchMap, tap } from 'rxjs';

type EpisodeState = Episode;

const initialData = (): EpisodeState => emptyEpisode();

@Injectable()
export class EpisodeStore extends ComponentStore<EpisodeState> {
  readonly #episodesService = inject(EpisodesService);

  constructor() {
    super(initialData());
  }

  readonly getEpisode = this.effect((id$: Observable<string>) =>
    id$.pipe(
      switchMap((id) => this.#episodesService.getEpisode(id)),
      tap((episode) => {
        this.patchState(episode);
      })
    )
  );
}