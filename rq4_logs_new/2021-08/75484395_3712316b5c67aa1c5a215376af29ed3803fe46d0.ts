/* tslint:disable */
/* eslint-disable */
// @ts-nocheck

import { ReaderFragment } from "relay-runtime";

import { FragmentRefs } from "relay-runtime";
export type notSeenEpisodeListItem = {
    readonly name: string | null;
    readonly airdate: unknown | null;
    readonly seasonAndNumber: string | null;
    readonly tvShow: {
        readonly id: string;
        readonly name: string | null;
    } | null;
    readonly " $refType": "notSeenEpisodeListItem";
};
export type notSeenEpisodeListItem$data = notSeenEpisodeListItem;
export type notSeenEpisodeListItem$key = {
    readonly " $data"?: notSeenEpisodeListItem$data;
    readonly " $fragmentRefs": FragmentRefs<"notSeenEpisodeListItem">;
};



const node: ReaderFragment = (function(){
var v0 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "name",
  "storageKey": null
};
return {
  "argumentDefinitions": [],
  "kind": "Fragment",
  "metadata": null,
  "name": "notSeenEpisodeListItem",
  "selections": [
    (v0/*: any*/),
    {
      "alias": null,
      "args": null,
      "kind": "ScalarField",
      "name": "airdate",
      "storageKey": null
    },
    {
      "alias": null,
      "args": null,
      "kind": "ScalarField",
      "name": "seasonAndNumber",
      "storageKey": null
    },
    {
      "alias": null,
      "args": null,
      "concreteType": "TvShow",
      "kind": "LinkedField",
      "name": "tvShow",
      "plural": false,
      "selections": [
        {
          "alias": null,
          "args": null,
          "kind": "ScalarField",
          "name": "id",
          "storageKey": null
        },
        (v0/*: any*/)
      ],
      "storageKey": null
    }
  ],
  "type": "Episode",
  "abstractKey": null
};
})();
(node as any).hash = '5ad5f5165231a351439cb8e831c52eb6';
export default node;