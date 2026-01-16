import { AssertOptions } from "../AssertOptions";
import { doWithOptions, FatalError } from "../util/retry";
import { SmokeTestWorld } from "../../../../smoke-test/features/support/world";
import { AllSdmGoals } from "../../../typings/types";
import { logger } from "@atomist/automation-client";
import SdmGoal = AllSdmGoals.SdmGoal;

export type SdmGoalState =
    "planned"
    | "requested"
    | "in_process"
    | "waiting_for_approval"
    | "success"
    | "failure"
    | "skipped";

export async function waitForGoalOf(world: SmokeTestWorld,
                                    sha: string,
                                    test: (g: SdmGoal) => boolean,
                                    state: SdmGoalState,
                                    opts?: AssertOptions): Promise<SdmGoal> {
    return doWithOptions(async () => {
            logger.debug(`Looking for goal satisfying [${test}] on commit ${sha}; options=${JSON.stringify(opts)}...`);
            const result = await world.graphClient.executeQueryFromFile<AllSdmGoals.Query, AllSdmGoals.Variables>(
                "src/graphql/query/AllSdmGoals", {
                    sha: [sha],
                });
            const goals: AllSdmGoals.SdmGoal[] = result.SdmGoal;
            const it = goals.find(test);
            logger.info(`Looking for goal satisfying [${test}] on commit ${sha}: Found ` + goalsString(goals));
            if (!it) {
                throw new Error(`Not there: Goal state satisfying [${test}] required on commit ${sha}: Found ` + goalsString(goals));
            }
            if (it.state === "failure" && state !== "failure") {
                throw new FatalError(`Failed state: Status satisfying [${test}] required on commit ${sha}: Found ` + goalsString(goals));
            }
            if (it.state !== state) {
                throw new Error(`Keeping looking: Status satisfying [${test}] required on commit ${sha}: Found ` + goalsString(goals));
            }
            return it;
        }, `Waiting for status satisfying [${test}] and state [${state}] on commit ${sha}`,
        opts);
}

function goalsString(goals: SdmGoal[]) {
    return `${goals.length} goals: \n${goals.map(goalString).join("\n\t")}`;
}

export function goalString(s: SdmGoal) {
    //return `context='${s.context}';state='${s.state}';target_url=${s.target_url};description='${s.description}'`;
    return JSON.stringify(s);
}