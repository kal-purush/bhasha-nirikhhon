import { Analysis } from "../../model/Analysis";

export function filterAnalyses(
    analyses: Analysis[],
    selectedTrialIds: string[],
    selectedExperimentalStrategies: string[],
    searchFilter: string
): Analysis[] {
    return analyses.filter((analysis: Analysis) => {
        let isTrialIdMatch = true;
        let isExperimentalStrategyMatch = true;
        let isSearchFilterMatch = true;
        if (selectedTrialIds.length > 0) {
            isTrialIdMatch = selectedTrialIds.includes(analysis.trial_name);
        }
        if (selectedExperimentalStrategies.length > 0) {
            isExperimentalStrategyMatch = selectedExperimentalStrategies.includes(
                analysis.experimental_strategy
            );
        }
        if (searchFilter.length > 0) {
            isSearchFilterMatch = analysis._id
                .toLowerCase()
                .includes(searchFilter.toLowerCase());
        }
        return (
            isTrialIdMatch && isExperimentalStrategyMatch && isSearchFilterMatch
        );
    });
}

export function changeOption(
    selectedOptions: string[],
    option: string
): string[] {
    if (selectedOptions.includes(option)) {
        return selectedOptions.filter((selectedOption: string) => {
            return selectedOption !== option;
        });
    } else {
        return [...selectedOptions, option];
    }
}