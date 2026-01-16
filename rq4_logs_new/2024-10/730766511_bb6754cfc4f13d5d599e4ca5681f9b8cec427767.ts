import { LookitWindow } from "@lookit/data/dist/types";
import Handlebars from "handlebars";
import { PluginInfo, TrialType } from "jspsych";
import consentDocumentTemplate from "../hbs/consent-document.hbs";
import consentVideoTrialTemplate from "../hbs/consent-video-trial.hbs";
import { initI18nAndTemplates } from "./utils";

declare const window: LookitWindow;

const video_container_id = "lookit-jspsych-video-container";

/**
 * Translate, render, and get consent document HTML.
 *
 * @param trial - JsPsych trial object containing trial params
 * @returns Consent document HTML
 */
export const consentVideo = (trial: TrialType<PluginInfo>) => {
  const experiment = window.chs.study.attributes;
  const { PIName, PIContact } = trial;

  initI18nAndTemplates(trial);

  const consent = Handlebars.compile(consentDocumentTemplate)({
    ...trial,
    name: PIName,
    contact: PIContact,
    experiment,
  });

  return Handlebars.compile(consentVideoTrialTemplate)({
    ...trial,
    consent,
    video_container_id,
  });
};