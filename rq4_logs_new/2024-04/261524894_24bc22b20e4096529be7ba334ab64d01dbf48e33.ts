import { ActionTriggers } from '@graasp/sdk';

export const actionsDescriptionTransKeys = Object.values(ActionTriggers).reduce(
  (acc: { [key: string]: string }, cur) => {
    acc[cur] = `${cur.toUpperCase().replaceAll('-', '_')}_ACTION_DESCRIPTION`;
    return acc;
  },
  {},
);