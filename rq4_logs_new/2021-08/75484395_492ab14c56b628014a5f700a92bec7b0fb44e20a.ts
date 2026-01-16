/* tslint:disable */
/* eslint-disable */
// @ts-nocheck

import { ReaderFragment } from "relay-runtime";
import { FragmentRefs } from "relay-runtime";
export type notSeenEpisodeList = {
    readonly notSeenEpisodes: {
        readonly edges: ReadonlyArray<{
            readonly node: {
                readonly id: string;
                readonly name: string | null;
                readonly airdate: unknown | null;
                readonly tvShow: {
                    readonly name: string | null;
                } | null;
            } | null;
        } | null> | null;
    } | null;
    readonly " $refType": "notSeenEpisodeList";
};
export type notSeenEpisodeList$data = notSeenEpisodeList;
export type notSeenEpisodeList$key = {
    readonly " $data"?: notSeenEpisodeList$data;
    readonly " $fragmentRefs": FragmentRefs<"notSeenEpisodeList">;
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
  "name": "notSeenEpisodeList",
  "selections": [
    {
      "alias": null,
      "args": null,
      "concreteType": "EpisodeConnection",
      "kind": "LinkedField",
      "name": "notSeenEpisodes",
      "plural": false,
      "selections": [
        {
          "alias": null,
          "args": null,
          "concreteType": "EpisodeEdge",
          "kind": "LinkedField",
          "name": "edges",
          "plural": true,
          "selections": [
            {
              "alias": null,
              "args": null,
              "concreteType": "Episode",
              "kind": "LinkedField",
              "name": "node",
              "plural": false,
              "selections": [
                {
                  "alias": null,
                  "args": null,
                  "kind": "ScalarField",
                  "name": "id",
                  "storageKey": null
                },
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
                  "concreteType": "TvShow",
                  "kind": "LinkedField",
                  "name": "tvShow",
                  "plural": false,
                  "selections": [
                    (v0/*: any*/)
                  ],
                  "storageKey": null
                }
              ],
              "storageKey": null
            }
          ],
          "storageKey": null
        }
      ],
      "storageKey": null
    }
  ],
  "type": "TvHelperViewer",
  "abstractKey": null
};
})();
(node as any).hash = 'bd4abf41e3aa87d29cf30db1943f8755';
export default node;