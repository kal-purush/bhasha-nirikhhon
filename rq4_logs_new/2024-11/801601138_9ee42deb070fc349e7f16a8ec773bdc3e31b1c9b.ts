import { components, paths } from "@/lib/api/generated/schema";

export type TwinClassCreateRq =
  paths["/private/twin_class/v1"]["post"]["requestBody"]["content"]["application/json"];
export type TwinClassUpdateRq = components["schemas"]["TwinClassUpdateRqV1"];

export type TwinClassField = components["schemas"]["TwinClassFieldV1"];
export type TwinClassFieldDescriptor =
  components["schemas"]["TwinClassFieldDescriptorDTO"];
export type TwinClassFieldCreateRq =
  components["schemas"]["TwinClassFieldCreateRqV1"];
export type TwinClassFieldUpdateRq =
  components["schemas"]["TwinClassFieldUpdateRqV1"];
// export type TwinClassFieldUpdateRq = components["schemas"]["TwinClassField"];
export type TwinClassStatusCreateRq =
  components["schemas"]["TwinStatusCreateRqV1"];
export type TwinClassStatusUpdateRq =
  components["schemas"]["TwinStatusUpdateRqV1"];
export type TwinFlow = components["schemas"]["TwinflowBaseV3"];
export type TwinFlowCreateRq = components["schemas"]["TwinflowCreateRqV1"];
export type TwinFlowUpdateRq = components["schemas"]["TwinflowUpdateRqV1"];
export type TwinFlowTransition =
  components["schemas"]["TwinflowTransitionBaseV3"];
export type TwinFlowTransitionCreateRq =
  components["schemas"]["TransitionCreateRqV1"];
export type TwinFlowTransitionUpdateRq =
  components["schemas"]["TransitionUpdateRqV1"];
export type TwinFlowTransitionTrigger = components["schemas"]["TriggerV1"];
export type TwinFlowTransitionTriggerCud =
  components["schemas"]["TriggerCudV1"];
export type TwinFlowTransitionTriggerUpdate =
  components["schemas"]["TriggerUpdateV1"];
export type TwinFlowTransitionValidator = components["schemas"]["ValidatorV1"];
export type TwinFlowTransitionValidatorCud =
  components["schemas"]["ValidatorCudV1"];
export type TwinFlowTransitionValidatorUpdate =
  components["schemas"]["ValidatorUpdateV1"];

export type TwinClassLink = components["schemas"]["LinkV1"];
export type TwinUpdateRq = components["schemas"]["TwinUpdateRqV1"];
export type TwinLinkView = components["schemas"]["TwinLinkViewV1"];

// export type TwinLinkAddV1 = components["schemas"]["TwinLinkAddV1"];
export type TwinLinkAddRqV1 = components["schemas"]["TwinLinkAddRqV1"];

export type CommentView = components["schemas"]["CommentViewV1"];

export type Featurer = components["schemas"]["FeaturerV1"];
export type FeaturerParam = components["schemas"]["FeaturerParamV1"];

export type RelatedObjects = components["schemas"]["RelatedObjectsV1"];