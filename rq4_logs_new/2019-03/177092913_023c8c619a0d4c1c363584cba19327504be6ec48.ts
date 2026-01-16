import { createSelector, MemoizedSelector } from '@ngrx/store';

import { EntityFeatureStoreSelectors } from './entity-feature-store';
import { StandardFeatureStoreSelectors } from './standard-feature-store';

export const selectError: MemoizedSelector<object, string> = createSelector(
  StandardFeatureStoreSelectors.selectStandardFeatureError,
  EntityFeatureStoreSelectors.selectEntityFeatureError,
  (standardFeatureError: string, entityFeatureError: string) => {
    return standardFeatureError || entityFeatureError;
  }
);

export const selectIsLoading: MemoizedSelector<object, boolean> = createSelector(
  StandardFeatureStoreSelectors.selectStandardFeatureIsLoading,
  EntityFeatureStoreSelectors.selectEntityFeatureIsLoading,
  (standardFeature: boolean, entityFeature: boolean) => {
    return standardFeature || entityFeature;
  }
);