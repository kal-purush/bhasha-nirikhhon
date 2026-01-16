/* tslint:disable */
/* eslint-disable */
// @ts-nocheck

import { ConcreteRequest } from "relay-runtime";
export type QuestUserProgressStates = "AVAILABLE" | "LOCKED" | "PASSED";
export type QuestEndingMutationVariables = {
    questId: unknown;
};
export type QuestEndingMutationResponse = {
    readonly user: {
        readonly completeQuest: {
            readonly record: {
                readonly username: string;
                readonly exp: number;
                readonly level: number;
                readonly completedQuests: ReadonlyArray<{
                    readonly id: string;
                    readonly questProgressState: QuestUserProgressStates;
                }>;
            };
        };
    };
};
export type QuestEndingMutation = {
    readonly response: QuestEndingMutationResponse;
    readonly variables: QuestEndingMutationVariables;
};



/*
mutation QuestEndingMutation(
  $questId: GlobalId!
) {
  user {
    completeQuest(questId: $questId) {
      record {
        username
        exp
        level
        completedQuests {
          id
          questProgressState
        }
        id
      }
    }
  }
}
*/

const node: ConcreteRequest = (function(){
var v0 = [
  {
    "defaultValue": null,
    "kind": "LocalArgument",
    "name": "questId"
  }
],
v1 = [
  {
    "kind": "Variable",
    "name": "questId",
    "variableName": "questId"
  }
],
v2 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "username",
  "storageKey": null
},
v3 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "exp",
  "storageKey": null
},
v4 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "level",
  "storageKey": null
},
v5 = {
  "alias": null,
  "args": null,
  "kind": "ScalarField",
  "name": "id",
  "storageKey": null
},
v6 = {
  "alias": null,
  "args": null,
  "concreteType": "Quest",
  "kind": "LinkedField",
  "name": "completedQuests",
  "plural": true,
  "selections": [
    (v5/*: any*/),
    {
      "alias": null,
      "args": null,
      "kind": "ScalarField",
      "name": "questProgressState",
      "storageKey": null
    }
  ],
  "storageKey": null
};
return {
  "fragment": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Fragment",
    "metadata": null,
    "name": "QuestEndingMutation",
    "selections": [
      {
        "alias": null,
        "args": null,
        "concreteType": "UserMutations",
        "kind": "LinkedField",
        "name": "user",
        "plural": false,
        "selections": [
          {
            "alias": null,
            "args": (v1/*: any*/),
            "concreteType": "UserCompleteQuestPayload",
            "kind": "LinkedField",
            "name": "completeQuest",
            "plural": false,
            "selections": [
              {
                "alias": null,
                "args": null,
                "concreteType": "User",
                "kind": "LinkedField",
                "name": "record",
                "plural": false,
                "selections": [
                  (v2/*: any*/),
                  (v3/*: any*/),
                  (v4/*: any*/),
                  (v6/*: any*/)
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
    "type": "Mutation",
    "abstractKey": null
  },
  "kind": "Request",
  "operation": {
    "argumentDefinitions": (v0/*: any*/),
    "kind": "Operation",
    "name": "QuestEndingMutation",
    "selections": [
      {
        "alias": null,
        "args": null,
        "concreteType": "UserMutations",
        "kind": "LinkedField",
        "name": "user",
        "plural": false,
        "selections": [
          {
            "alias": null,
            "args": (v1/*: any*/),
            "concreteType": "UserCompleteQuestPayload",
            "kind": "LinkedField",
            "name": "completeQuest",
            "plural": false,
            "selections": [
              {
                "alias": null,
                "args": null,
                "concreteType": "User",
                "kind": "LinkedField",
                "name": "record",
                "plural": false,
                "selections": [
                  (v2/*: any*/),
                  (v3/*: any*/),
                  (v4/*: any*/),
                  (v6/*: any*/),
                  (v5/*: any*/)
                ],
                "storageKey": null
              }
            ],
            "storageKey": null
          }
        ],
        "storageKey": null
      }
    ]
  },
  "params": {
    "cacheID": "0de4a745629b466bb1d4ca32cdf04c3c",
    "id": null,
    "metadata": {},
    "name": "QuestEndingMutation",
    "operationKind": "mutation",
    "text": "mutation QuestEndingMutation(\n  $questId: GlobalId!\n) {\n  user {\n    completeQuest(questId: $questId) {\n      record {\n        username\n        exp\n        level\n        completedQuests {\n          id\n          questProgressState\n        }\n        id\n      }\n    }\n  }\n}\n"
  }
};
})();
(node as any).hash = '20c5b8273c794d8415c569979bd974b4';
export default node;