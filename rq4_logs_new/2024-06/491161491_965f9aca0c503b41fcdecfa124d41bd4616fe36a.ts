/**
 * Copyright (c) HashiCorp, Inc.
 * SPDX-License-Identifier: MPL-2.0
 */

// https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule
// generated from terraform resource schema

import { Construct } from 'constructs';
import * as cdktf from 'cdktf';

// Configuration

export interface OceanRightSizingRuleConfig extends cdktf.TerraformMetaArguments {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#id OceanRightSizingRule#id}
  *
  * Please be aware that the id field is automatically added to all resources in Terraform providers using a Terraform provider SDK version below 2.
  * If you experience problems setting this value it might not be settable. Please take a look at the provider documentation to ensure it should be settable.
  */
  readonly id?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#ocean_id OceanRightSizingRule#ocean_id}
  */
  readonly oceanId?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#restart_pods OceanRightSizingRule#restart_pods}
  */
  readonly restartPods?: boolean | cdktf.IResolvable;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#rule_name OceanRightSizingRule#rule_name}
  */
  readonly ruleName: string;
  /**
  * attach_workloads block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#attach_workloads OceanRightSizingRule#attach_workloads}
  */
  readonly attachWorkloads?: OceanRightSizingRuleAttachWorkloads[] | cdktf.IResolvable;
  /**
  * detach_workloads block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#detach_workloads OceanRightSizingRule#detach_workloads}
  */
  readonly detachWorkloads?: OceanRightSizingRuleDetachWorkloads[] | cdktf.IResolvable;
  /**
  * recommendation_application_boundaries block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#recommendation_application_boundaries OceanRightSizingRule#recommendation_application_boundaries}
  */
  readonly recommendationApplicationBoundaries?: OceanRightSizingRuleRecommendationApplicationBoundaries[] | cdktf.IResolvable;
  /**
  * recommendation_application_intervals block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#recommendation_application_intervals OceanRightSizingRule#recommendation_application_intervals}
  */
  readonly recommendationApplicationIntervals: OceanRightSizingRuleRecommendationApplicationIntervals[] | cdktf.IResolvable;
  /**
  * recommendation_application_min_threshold block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#recommendation_application_min_threshold OceanRightSizingRule#recommendation_application_min_threshold}
  */
  readonly recommendationApplicationMinThreshold?: OceanRightSizingRuleRecommendationApplicationMinThreshold[] | cdktf.IResolvable;
  /**
  * recommendation_application_overhead_values block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#recommendation_application_overhead_values OceanRightSizingRule#recommendation_application_overhead_values}
  */
  readonly recommendationApplicationOverheadValues?: OceanRightSizingRuleRecommendationApplicationOverheadValues[] | cdktf.IResolvable;
}
export interface OceanRightSizingRuleAttachWorkloadsNamespacesLabels {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#key OceanRightSizingRule#key}
  */
  readonly key: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#value OceanRightSizingRule#value}
  */
  readonly value: string;
}

export function oceanRightSizingRuleAttachWorkloadsNamespacesLabelsToTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespacesLabels | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    key: cdktf.stringToTerraform(struct!.key),
    value: cdktf.stringToTerraform(struct!.value),
  }
}


export function oceanRightSizingRuleAttachWorkloadsNamespacesLabelsToHclTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespacesLabels | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    key: {
      value: cdktf.stringToHclTerraform(struct!.key),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    value: {
      value: cdktf.stringToHclTerraform(struct!.value),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesLabelsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleAttachWorkloadsNamespacesLabels | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._key !== undefined) {
      hasAnyValues = true;
      internalValueResult.key = this._key;
    }
    if (this._value !== undefined) {
      hasAnyValues = true;
      internalValueResult.value = this._value;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleAttachWorkloadsNamespacesLabels | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._key = undefined;
      this._value = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._key = value.key;
      this._value = value.value;
    }
  }

  // key - computed: false, optional: false, required: true
  private _key?: string; 
  public get key() {
    return this.getStringAttribute('key');
  }
  public set key(value: string) {
    this._key = value;
  }
  // Temporarily expose input value. Use with caution.
  public get keyInput() {
    return this._key;
  }

  // value - computed: false, optional: false, required: true
  private _value?: string; 
  public get value() {
    return this.getStringAttribute('value');
  }
  public set value(value: string) {
    this._value = value;
  }
  // Temporarily expose input value. Use with caution.
  public get valueInput() {
    return this._value;
  }
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesLabelsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleAttachWorkloadsNamespacesLabels[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleAttachWorkloadsNamespacesLabelsOutputReference {
    return new OceanRightSizingRuleAttachWorkloadsNamespacesLabelsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#regex_name OceanRightSizingRule#regex_name}
  */
  readonly regexName?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workload_name OceanRightSizingRule#workload_name}
  */
  readonly workloadName?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workload_type OceanRightSizingRule#workload_type}
  */
  readonly workloadType: string;
}

export function oceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsToTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    regex_name: cdktf.stringToTerraform(struct!.regexName),
    workload_name: cdktf.stringToTerraform(struct!.workloadName),
    workload_type: cdktf.stringToTerraform(struct!.workloadType),
  }
}


export function oceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsToHclTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    regex_name: {
      value: cdktf.stringToHclTerraform(struct!.regexName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    workload_name: {
      value: cdktf.stringToHclTerraform(struct!.workloadName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    workload_type: {
      value: cdktf.stringToHclTerraform(struct!.workloadType),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._regexName !== undefined) {
      hasAnyValues = true;
      internalValueResult.regexName = this._regexName;
    }
    if (this._workloadName !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloadName = this._workloadName;
    }
    if (this._workloadType !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloadType = this._workloadType;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._regexName = undefined;
      this._workloadName = undefined;
      this._workloadType = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._regexName = value.regexName;
      this._workloadName = value.workloadName;
      this._workloadType = value.workloadType;
    }
  }

  // regex_name - computed: false, optional: true, required: false
  private _regexName?: string; 
  public get regexName() {
    return this.getStringAttribute('regex_name');
  }
  public set regexName(value: string) {
    this._regexName = value;
  }
  public resetRegexName() {
    this._regexName = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get regexNameInput() {
    return this._regexName;
  }

  // workload_name - computed: false, optional: true, required: false
  private _workloadName?: string; 
  public get workloadName() {
    return this.getStringAttribute('workload_name');
  }
  public set workloadName(value: string) {
    this._workloadName = value;
  }
  public resetWorkloadName() {
    this._workloadName = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadNameInput() {
    return this._workloadName;
  }

  // workload_type - computed: false, optional: false, required: true
  private _workloadType?: string; 
  public get workloadType() {
    return this.getStringAttribute('workload_type');
  }
  public set workloadType(value: string) {
    this._workloadType = value;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadTypeInput() {
    return this._workloadType;
  }
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsOutputReference {
    return new OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleAttachWorkloadsNamespaces {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#namespace_name OceanRightSizingRule#namespace_name}
  */
  readonly namespaceName: string;
  /**
  * labels block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#labels OceanRightSizingRule#labels}
  */
  readonly labels?: OceanRightSizingRuleAttachWorkloadsNamespacesLabels[] | cdktf.IResolvable;
  /**
  * workloads block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workloads OceanRightSizingRule#workloads}
  */
  readonly workloads?: OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleAttachWorkloadsNamespacesToTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespaces | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    namespace_name: cdktf.stringToTerraform(struct!.namespaceName),
    labels: cdktf.listMapper(oceanRightSizingRuleAttachWorkloadsNamespacesLabelsToTerraform, true)(struct!.labels),
    workloads: cdktf.listMapper(oceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsToTerraform, true)(struct!.workloads),
  }
}


export function oceanRightSizingRuleAttachWorkloadsNamespacesToHclTerraform(struct?: OceanRightSizingRuleAttachWorkloadsNamespaces | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    namespace_name: {
      value: cdktf.stringToHclTerraform(struct!.namespaceName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    labels: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleAttachWorkloadsNamespacesLabelsToHclTerraform, true)(struct!.labels),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleAttachWorkloadsNamespacesLabelsList",
    },
    workloads: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsToHclTerraform, true)(struct!.workloads),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleAttachWorkloadsNamespaces | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._namespaceName !== undefined) {
      hasAnyValues = true;
      internalValueResult.namespaceName = this._namespaceName;
    }
    if (this._labels?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.labels = this._labels?.internalValue;
    }
    if (this._workloads?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloads = this._workloads?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleAttachWorkloadsNamespaces | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._namespaceName = undefined;
      this._labels.internalValue = undefined;
      this._workloads.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._namespaceName = value.namespaceName;
      this._labels.internalValue = value.labels;
      this._workloads.internalValue = value.workloads;
    }
  }

  // namespace_name - computed: false, optional: false, required: true
  private _namespaceName?: string; 
  public get namespaceName() {
    return this.getStringAttribute('namespace_name');
  }
  public set namespaceName(value: string) {
    this._namespaceName = value;
  }
  // Temporarily expose input value. Use with caution.
  public get namespaceNameInput() {
    return this._namespaceName;
  }

  // labels - computed: false, optional: true, required: false
  private _labels = new OceanRightSizingRuleAttachWorkloadsNamespacesLabelsList(this, "labels", true);
  public get labels() {
    return this._labels;
  }
  public putLabels(value: OceanRightSizingRuleAttachWorkloadsNamespacesLabels[] | cdktf.IResolvable) {
    this._labels.internalValue = value;
  }
  public resetLabels() {
    this._labels.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get labelsInput() {
    return this._labels.internalValue;
  }

  // workloads - computed: false, optional: true, required: false
  private _workloads = new OceanRightSizingRuleAttachWorkloadsNamespacesWorkloadsList(this, "workloads", true);
  public get workloads() {
    return this._workloads;
  }
  public putWorkloads(value: OceanRightSizingRuleAttachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable) {
    this._workloads.internalValue = value;
  }
  public resetWorkloads() {
    this._workloads.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadsInput() {
    return this._workloads.internalValue;
  }
}

export class OceanRightSizingRuleAttachWorkloadsNamespacesList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleAttachWorkloadsNamespaces[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleAttachWorkloadsNamespacesOutputReference {
    return new OceanRightSizingRuleAttachWorkloadsNamespacesOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleAttachWorkloads {
  /**
  * namespaces block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#namespaces OceanRightSizingRule#namespaces}
  */
  readonly namespaces: OceanRightSizingRuleAttachWorkloadsNamespaces[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleAttachWorkloadsToTerraform(struct?: OceanRightSizingRuleAttachWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    namespaces: cdktf.listMapper(oceanRightSizingRuleAttachWorkloadsNamespacesToTerraform, true)(struct!.namespaces),
  }
}


export function oceanRightSizingRuleAttachWorkloadsToHclTerraform(struct?: OceanRightSizingRuleAttachWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    namespaces: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleAttachWorkloadsNamespacesToHclTerraform, true)(struct!.namespaces),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleAttachWorkloadsNamespacesList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleAttachWorkloadsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleAttachWorkloads | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._namespaces?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.namespaces = this._namespaces?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleAttachWorkloads | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._namespaces.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._namespaces.internalValue = value.namespaces;
    }
  }

  // namespaces - computed: false, optional: false, required: true
  private _namespaces = new OceanRightSizingRuleAttachWorkloadsNamespacesList(this, "namespaces", true);
  public get namespaces() {
    return this._namespaces;
  }
  public putNamespaces(value: OceanRightSizingRuleAttachWorkloadsNamespaces[] | cdktf.IResolvable) {
    this._namespaces.internalValue = value;
  }
  // Temporarily expose input value. Use with caution.
  public get namespacesInput() {
    return this._namespaces.internalValue;
  }
}

export class OceanRightSizingRuleAttachWorkloadsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleAttachWorkloads[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleAttachWorkloadsOutputReference {
    return new OceanRightSizingRuleAttachWorkloadsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleDetachWorkloadsNamespacesLabels {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#key OceanRightSizingRule#key}
  */
  readonly key: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#value OceanRightSizingRule#value}
  */
  readonly value: string;
}

export function oceanRightSizingRuleDetachWorkloadsNamespacesLabelsToTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespacesLabels | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    key: cdktf.stringToTerraform(struct!.key),
    value: cdktf.stringToTerraform(struct!.value),
  }
}


export function oceanRightSizingRuleDetachWorkloadsNamespacesLabelsToHclTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespacesLabels | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    key: {
      value: cdktf.stringToHclTerraform(struct!.key),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    value: {
      value: cdktf.stringToHclTerraform(struct!.value),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesLabelsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleDetachWorkloadsNamespacesLabels | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._key !== undefined) {
      hasAnyValues = true;
      internalValueResult.key = this._key;
    }
    if (this._value !== undefined) {
      hasAnyValues = true;
      internalValueResult.value = this._value;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleDetachWorkloadsNamespacesLabels | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._key = undefined;
      this._value = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._key = value.key;
      this._value = value.value;
    }
  }

  // key - computed: false, optional: false, required: true
  private _key?: string; 
  public get key() {
    return this.getStringAttribute('key');
  }
  public set key(value: string) {
    this._key = value;
  }
  // Temporarily expose input value. Use with caution.
  public get keyInput() {
    return this._key;
  }

  // value - computed: false, optional: false, required: true
  private _value?: string; 
  public get value() {
    return this.getStringAttribute('value');
  }
  public set value(value: string) {
    this._value = value;
  }
  // Temporarily expose input value. Use with caution.
  public get valueInput() {
    return this._value;
  }
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesLabelsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleDetachWorkloadsNamespacesLabels[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleDetachWorkloadsNamespacesLabelsOutputReference {
    return new OceanRightSizingRuleDetachWorkloadsNamespacesLabelsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#regex_name OceanRightSizingRule#regex_name}
  */
  readonly regexName?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workload_name OceanRightSizingRule#workload_name}
  */
  readonly workloadName?: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workload_type OceanRightSizingRule#workload_type}
  */
  readonly workloadType: string;
}

export function oceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsToTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    regex_name: cdktf.stringToTerraform(struct!.regexName),
    workload_name: cdktf.stringToTerraform(struct!.workloadName),
    workload_type: cdktf.stringToTerraform(struct!.workloadType),
  }
}


export function oceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsToHclTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    regex_name: {
      value: cdktf.stringToHclTerraform(struct!.regexName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    workload_name: {
      value: cdktf.stringToHclTerraform(struct!.workloadName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    workload_type: {
      value: cdktf.stringToHclTerraform(struct!.workloadType),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._regexName !== undefined) {
      hasAnyValues = true;
      internalValueResult.regexName = this._regexName;
    }
    if (this._workloadName !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloadName = this._workloadName;
    }
    if (this._workloadType !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloadType = this._workloadType;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._regexName = undefined;
      this._workloadName = undefined;
      this._workloadType = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._regexName = value.regexName;
      this._workloadName = value.workloadName;
      this._workloadType = value.workloadType;
    }
  }

  // regex_name - computed: false, optional: true, required: false
  private _regexName?: string; 
  public get regexName() {
    return this.getStringAttribute('regex_name');
  }
  public set regexName(value: string) {
    this._regexName = value;
  }
  public resetRegexName() {
    this._regexName = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get regexNameInput() {
    return this._regexName;
  }

  // workload_name - computed: false, optional: true, required: false
  private _workloadName?: string; 
  public get workloadName() {
    return this.getStringAttribute('workload_name');
  }
  public set workloadName(value: string) {
    this._workloadName = value;
  }
  public resetWorkloadName() {
    this._workloadName = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadNameInput() {
    return this._workloadName;
  }

  // workload_type - computed: false, optional: false, required: true
  private _workloadType?: string; 
  public get workloadType() {
    return this.getStringAttribute('workload_type');
  }
  public set workloadType(value: string) {
    this._workloadType = value;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadTypeInput() {
    return this._workloadType;
  }
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsOutputReference {
    return new OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleDetachWorkloadsNamespaces {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#namespace_name OceanRightSizingRule#namespace_name}
  */
  readonly namespaceName: string;
  /**
  * labels block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#labels OceanRightSizingRule#labels}
  */
  readonly labels?: OceanRightSizingRuleDetachWorkloadsNamespacesLabels[] | cdktf.IResolvable;
  /**
  * workloads block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#workloads OceanRightSizingRule#workloads}
  */
  readonly workloads?: OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleDetachWorkloadsNamespacesToTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespaces | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    namespace_name: cdktf.stringToTerraform(struct!.namespaceName),
    labels: cdktf.listMapper(oceanRightSizingRuleDetachWorkloadsNamespacesLabelsToTerraform, true)(struct!.labels),
    workloads: cdktf.listMapper(oceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsToTerraform, true)(struct!.workloads),
  }
}


export function oceanRightSizingRuleDetachWorkloadsNamespacesToHclTerraform(struct?: OceanRightSizingRuleDetachWorkloadsNamespaces | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    namespace_name: {
      value: cdktf.stringToHclTerraform(struct!.namespaceName),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    labels: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleDetachWorkloadsNamespacesLabelsToHclTerraform, true)(struct!.labels),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleDetachWorkloadsNamespacesLabelsList",
    },
    workloads: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsToHclTerraform, true)(struct!.workloads),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleDetachWorkloadsNamespaces | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._namespaceName !== undefined) {
      hasAnyValues = true;
      internalValueResult.namespaceName = this._namespaceName;
    }
    if (this._labels?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.labels = this._labels?.internalValue;
    }
    if (this._workloads?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.workloads = this._workloads?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleDetachWorkloadsNamespaces | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._namespaceName = undefined;
      this._labels.internalValue = undefined;
      this._workloads.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._namespaceName = value.namespaceName;
      this._labels.internalValue = value.labels;
      this._workloads.internalValue = value.workloads;
    }
  }

  // namespace_name - computed: false, optional: false, required: true
  private _namespaceName?: string; 
  public get namespaceName() {
    return this.getStringAttribute('namespace_name');
  }
  public set namespaceName(value: string) {
    this._namespaceName = value;
  }
  // Temporarily expose input value. Use with caution.
  public get namespaceNameInput() {
    return this._namespaceName;
  }

  // labels - computed: false, optional: true, required: false
  private _labels = new OceanRightSizingRuleDetachWorkloadsNamespacesLabelsList(this, "labels", true);
  public get labels() {
    return this._labels;
  }
  public putLabels(value: OceanRightSizingRuleDetachWorkloadsNamespacesLabels[] | cdktf.IResolvable) {
    this._labels.internalValue = value;
  }
  public resetLabels() {
    this._labels.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get labelsInput() {
    return this._labels.internalValue;
  }

  // workloads - computed: false, optional: true, required: false
  private _workloads = new OceanRightSizingRuleDetachWorkloadsNamespacesWorkloadsList(this, "workloads", true);
  public get workloads() {
    return this._workloads;
  }
  public putWorkloads(value: OceanRightSizingRuleDetachWorkloadsNamespacesWorkloads[] | cdktf.IResolvable) {
    this._workloads.internalValue = value;
  }
  public resetWorkloads() {
    this._workloads.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get workloadsInput() {
    return this._workloads.internalValue;
  }
}

export class OceanRightSizingRuleDetachWorkloadsNamespacesList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleDetachWorkloadsNamespaces[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleDetachWorkloadsNamespacesOutputReference {
    return new OceanRightSizingRuleDetachWorkloadsNamespacesOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleDetachWorkloads {
  /**
  * namespaces block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#namespaces OceanRightSizingRule#namespaces}
  */
  readonly namespaces: OceanRightSizingRuleDetachWorkloadsNamespaces[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleDetachWorkloadsToTerraform(struct?: OceanRightSizingRuleDetachWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    namespaces: cdktf.listMapper(oceanRightSizingRuleDetachWorkloadsNamespacesToTerraform, true)(struct!.namespaces),
  }
}


export function oceanRightSizingRuleDetachWorkloadsToHclTerraform(struct?: OceanRightSizingRuleDetachWorkloads | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    namespaces: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleDetachWorkloadsNamespacesToHclTerraform, true)(struct!.namespaces),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleDetachWorkloadsNamespacesList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleDetachWorkloadsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleDetachWorkloads | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._namespaces?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.namespaces = this._namespaces?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleDetachWorkloads | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._namespaces.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._namespaces.internalValue = value.namespaces;
    }
  }

  // namespaces - computed: false, optional: false, required: true
  private _namespaces = new OceanRightSizingRuleDetachWorkloadsNamespacesList(this, "namespaces", true);
  public get namespaces() {
    return this._namespaces;
  }
  public putNamespaces(value: OceanRightSizingRuleDetachWorkloadsNamespaces[] | cdktf.IResolvable) {
    this._namespaces.internalValue = value;
  }
  // Temporarily expose input value. Use with caution.
  public get namespacesInput() {
    return this._namespaces.internalValue;
  }
}

export class OceanRightSizingRuleDetachWorkloadsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleDetachWorkloads[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleDetachWorkloadsOutputReference {
    return new OceanRightSizingRuleDetachWorkloadsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationBoundaries {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#cpu_max OceanRightSizingRule#cpu_max}
  */
  readonly cpuMax?: number;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#cpu_min OceanRightSizingRule#cpu_min}
  */
  readonly cpuMin?: number;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#memory_max OceanRightSizingRule#memory_max}
  */
  readonly memoryMax?: number;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#memory_min OceanRightSizingRule#memory_min}
  */
  readonly memoryMin?: number;
}

export function oceanRightSizingRuleRecommendationApplicationBoundariesToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationBoundaries | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    cpu_max: cdktf.numberToTerraform(struct!.cpuMax),
    cpu_min: cdktf.numberToTerraform(struct!.cpuMin),
    memory_max: cdktf.numberToTerraform(struct!.memoryMax),
    memory_min: cdktf.numberToTerraform(struct!.memoryMin),
  }
}


export function oceanRightSizingRuleRecommendationApplicationBoundariesToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationBoundaries | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    cpu_max: {
      value: cdktf.numberToHclTerraform(struct!.cpuMax),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
    cpu_min: {
      value: cdktf.numberToHclTerraform(struct!.cpuMin),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
    memory_max: {
      value: cdktf.numberToHclTerraform(struct!.memoryMax),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
    memory_min: {
      value: cdktf.numberToHclTerraform(struct!.memoryMin),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationBoundariesOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationBoundaries | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._cpuMax !== undefined) {
      hasAnyValues = true;
      internalValueResult.cpuMax = this._cpuMax;
    }
    if (this._cpuMin !== undefined) {
      hasAnyValues = true;
      internalValueResult.cpuMin = this._cpuMin;
    }
    if (this._memoryMax !== undefined) {
      hasAnyValues = true;
      internalValueResult.memoryMax = this._memoryMax;
    }
    if (this._memoryMin !== undefined) {
      hasAnyValues = true;
      internalValueResult.memoryMin = this._memoryMin;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationBoundaries | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._cpuMax = undefined;
      this._cpuMin = undefined;
      this._memoryMax = undefined;
      this._memoryMin = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._cpuMax = value.cpuMax;
      this._cpuMin = value.cpuMin;
      this._memoryMax = value.memoryMax;
      this._memoryMin = value.memoryMin;
    }
  }

  // cpu_max - computed: false, optional: true, required: false
  private _cpuMax?: number; 
  public get cpuMax() {
    return this.getNumberAttribute('cpu_max');
  }
  public set cpuMax(value: number) {
    this._cpuMax = value;
  }
  public resetCpuMax() {
    this._cpuMax = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get cpuMaxInput() {
    return this._cpuMax;
  }

  // cpu_min - computed: false, optional: true, required: false
  private _cpuMin?: number; 
  public get cpuMin() {
    return this.getNumberAttribute('cpu_min');
  }
  public set cpuMin(value: number) {
    this._cpuMin = value;
  }
  public resetCpuMin() {
    this._cpuMin = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get cpuMinInput() {
    return this._cpuMin;
  }

  // memory_max - computed: false, optional: true, required: false
  private _memoryMax?: number; 
  public get memoryMax() {
    return this.getNumberAttribute('memory_max');
  }
  public set memoryMax(value: number) {
    this._memoryMax = value;
  }
  public resetMemoryMax() {
    this._memoryMax = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get memoryMaxInput() {
    return this._memoryMax;
  }

  // memory_min - computed: false, optional: true, required: false
  private _memoryMin?: number; 
  public get memoryMin() {
    return this.getNumberAttribute('memory_min');
  }
  public set memoryMin(value: number) {
    this._memoryMin = value;
  }
  public resetMemoryMin() {
    this._memoryMin = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get memoryMinInput() {
    return this._memoryMin;
  }
}

export class OceanRightSizingRuleRecommendationApplicationBoundariesList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationBoundaries[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationBoundariesOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationBoundariesOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_days OceanRightSizingRule#interval_days}
  */
  readonly intervalDays: string[];
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_hours_end_time OceanRightSizingRule#interval_hours_end_time}
  */
  readonly intervalHoursEndTime: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_hours_start_time OceanRightSizingRule#interval_hours_start_time}
  */
  readonly intervalHoursStartTime: string;
}

export function oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    interval_days: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.intervalDays),
    interval_hours_end_time: cdktf.stringToTerraform(struct!.intervalHoursEndTime),
    interval_hours_start_time: cdktf.stringToTerraform(struct!.intervalHoursStartTime),
  }
}


export function oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    interval_days: {
      value: cdktf.listMapperHcl(cdktf.stringToHclTerraform, false)(struct!.intervalDays),
      isBlock: false,
      type: "list",
      storageClassType: "stringList",
    },
    interval_hours_end_time: {
      value: cdktf.stringToHclTerraform(struct!.intervalHoursEndTime),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    interval_hours_start_time: {
      value: cdktf.stringToHclTerraform(struct!.intervalHoursStartTime),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._intervalDays !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalDays = this._intervalDays;
    }
    if (this._intervalHoursEndTime !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalHoursEndTime = this._intervalHoursEndTime;
    }
    if (this._intervalHoursStartTime !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalHoursStartTime = this._intervalHoursStartTime;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._intervalDays = undefined;
      this._intervalHoursEndTime = undefined;
      this._intervalHoursStartTime = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._intervalDays = value.intervalDays;
      this._intervalHoursEndTime = value.intervalHoursEndTime;
      this._intervalHoursStartTime = value.intervalHoursStartTime;
    }
  }

  // interval_days - computed: false, optional: false, required: true
  private _intervalDays?: string[]; 
  public get intervalDays() {
    return this.getListAttribute('interval_days');
  }
  public set intervalDays(value: string[]) {
    this._intervalDays = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalDaysInput() {
    return this._intervalDays;
  }

  // interval_hours_end_time - computed: false, optional: false, required: true
  private _intervalHoursEndTime?: string; 
  public get intervalHoursEndTime() {
    return this.getStringAttribute('interval_hours_end_time');
  }
  public set intervalHoursEndTime(value: string) {
    this._intervalHoursEndTime = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalHoursEndTimeInput() {
    return this._intervalHoursEndTime;
  }

  // interval_hours_start_time - computed: false, optional: false, required: true
  private _intervalHoursStartTime?: string; 
  public get intervalHoursStartTime() {
    return this.getStringAttribute('interval_hours_start_time');
  }
  public set intervalHoursStartTime(value: string) {
    this._intervalHoursStartTime = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalHoursStartTimeInput() {
    return this._intervalHoursStartTime;
  }
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_months OceanRightSizingRule#interval_months}
  */
  readonly intervalMonths: number[];
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#week_of_the_month OceanRightSizingRule#week_of_the_month}
  */
  readonly weekOfTheMonth: string[];
  /**
  * weekly_repetition_basis block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#weekly_repetition_basis OceanRightSizingRule#weekly_repetition_basis}
  */
  readonly weeklyRepetitionBasis?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    interval_months: cdktf.listMapper(cdktf.numberToTerraform, false)(struct!.intervalMonths),
    week_of_the_month: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.weekOfTheMonth),
    weekly_repetition_basis: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisToTerraform, true)(struct!.weeklyRepetitionBasis),
  }
}


export function oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    interval_months: {
      value: cdktf.listMapperHcl(cdktf.numberToHclTerraform, false)(struct!.intervalMonths),
      isBlock: false,
      type: "list",
      storageClassType: "numberList",
    },
    week_of_the_month: {
      value: cdktf.listMapperHcl(cdktf.stringToHclTerraform, false)(struct!.weekOfTheMonth),
      isBlock: false,
      type: "list",
      storageClassType: "stringList",
    },
    weekly_repetition_basis: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisToHclTerraform, true)(struct!.weeklyRepetitionBasis),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._intervalMonths !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalMonths = this._intervalMonths;
    }
    if (this._weekOfTheMonth !== undefined) {
      hasAnyValues = true;
      internalValueResult.weekOfTheMonth = this._weekOfTheMonth;
    }
    if (this._weeklyRepetitionBasis?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.weeklyRepetitionBasis = this._weeklyRepetitionBasis?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._intervalMonths = undefined;
      this._weekOfTheMonth = undefined;
      this._weeklyRepetitionBasis.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._intervalMonths = value.intervalMonths;
      this._weekOfTheMonth = value.weekOfTheMonth;
      this._weeklyRepetitionBasis.internalValue = value.weeklyRepetitionBasis;
    }
  }

  // interval_months - computed: false, optional: false, required: true
  private _intervalMonths?: number[]; 
  public get intervalMonths() {
    return this.getNumberListAttribute('interval_months');
  }
  public set intervalMonths(value: number[]) {
    this._intervalMonths = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalMonthsInput() {
    return this._intervalMonths;
  }

  // week_of_the_month - computed: false, optional: false, required: true
  private _weekOfTheMonth?: string[]; 
  public get weekOfTheMonth() {
    return this.getListAttribute('week_of_the_month');
  }
  public set weekOfTheMonth(value: string[]) {
    this._weekOfTheMonth = value;
  }
  // Temporarily expose input value. Use with caution.
  public get weekOfTheMonthInput() {
    return this._weekOfTheMonth;
  }

  // weekly_repetition_basis - computed: false, optional: true, required: false
  private _weeklyRepetitionBasis = new OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasisList(this, "weekly_repetition_basis", true);
  public get weeklyRepetitionBasis() {
    return this._weeklyRepetitionBasis;
  }
  public putWeeklyRepetitionBasis(value: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisWeeklyRepetitionBasis[] | cdktf.IResolvable) {
    this._weeklyRepetitionBasis.internalValue = value;
  }
  public resetWeeklyRepetitionBasis() {
    this._weeklyRepetitionBasis.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get weeklyRepetitionBasisInput() {
    return this._weeklyRepetitionBasis.internalValue;
  }
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_days OceanRightSizingRule#interval_days}
  */
  readonly intervalDays: string[];
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_hours_end_time OceanRightSizingRule#interval_hours_end_time}
  */
  readonly intervalHoursEndTime: string;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#interval_hours_start_time OceanRightSizingRule#interval_hours_start_time}
  */
  readonly intervalHoursStartTime: string;
}

export function oceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    interval_days: cdktf.listMapper(cdktf.stringToTerraform, false)(struct!.intervalDays),
    interval_hours_end_time: cdktf.stringToTerraform(struct!.intervalHoursEndTime),
    interval_hours_start_time: cdktf.stringToTerraform(struct!.intervalHoursStartTime),
  }
}


export function oceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    interval_days: {
      value: cdktf.listMapperHcl(cdktf.stringToHclTerraform, false)(struct!.intervalDays),
      isBlock: false,
      type: "list",
      storageClassType: "stringList",
    },
    interval_hours_end_time: {
      value: cdktf.stringToHclTerraform(struct!.intervalHoursEndTime),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    interval_hours_start_time: {
      value: cdktf.stringToHclTerraform(struct!.intervalHoursStartTime),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._intervalDays !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalDays = this._intervalDays;
    }
    if (this._intervalHoursEndTime !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalHoursEndTime = this._intervalHoursEndTime;
    }
    if (this._intervalHoursStartTime !== undefined) {
      hasAnyValues = true;
      internalValueResult.intervalHoursStartTime = this._intervalHoursStartTime;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._intervalDays = undefined;
      this._intervalHoursEndTime = undefined;
      this._intervalHoursStartTime = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._intervalDays = value.intervalDays;
      this._intervalHoursEndTime = value.intervalHoursEndTime;
      this._intervalHoursStartTime = value.intervalHoursStartTime;
    }
  }

  // interval_days - computed: false, optional: false, required: true
  private _intervalDays?: string[]; 
  public get intervalDays() {
    return this.getListAttribute('interval_days');
  }
  public set intervalDays(value: string[]) {
    this._intervalDays = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalDaysInput() {
    return this._intervalDays;
  }

  // interval_hours_end_time - computed: false, optional: false, required: true
  private _intervalHoursEndTime?: string; 
  public get intervalHoursEndTime() {
    return this.getStringAttribute('interval_hours_end_time');
  }
  public set intervalHoursEndTime(value: string) {
    this._intervalHoursEndTime = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalHoursEndTimeInput() {
    return this._intervalHoursEndTime;
  }

  // interval_hours_start_time - computed: false, optional: false, required: true
  private _intervalHoursStartTime?: string; 
  public get intervalHoursStartTime() {
    return this.getStringAttribute('interval_hours_start_time');
  }
  public set intervalHoursStartTime(value: string) {
    this._intervalHoursStartTime = value;
  }
  // Temporarily expose input value. Use with caution.
  public get intervalHoursStartTimeInput() {
    return this._intervalHoursStartTime;
  }
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationIntervals {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#repetition_basis OceanRightSizingRule#repetition_basis}
  */
  readonly repetitionBasis: string;
  /**
  * monthly_repetition_basis block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#monthly_repetition_basis OceanRightSizingRule#monthly_repetition_basis}
  */
  readonly monthlyRepetitionBasis?: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis[] | cdktf.IResolvable;
  /**
  * weekly_repetition_basis block
  *
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#weekly_repetition_basis OceanRightSizingRule#weekly_repetition_basis}
  */
  readonly weeklyRepetitionBasis?: OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis[] | cdktf.IResolvable;
}

export function oceanRightSizingRuleRecommendationApplicationIntervalsToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervals | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    repetition_basis: cdktf.stringToTerraform(struct!.repetitionBasis),
    monthly_repetition_basis: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisToTerraform, true)(struct!.monthlyRepetitionBasis),
    weekly_repetition_basis: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisToTerraform, true)(struct!.weeklyRepetitionBasis),
  }
}


export function oceanRightSizingRuleRecommendationApplicationIntervalsToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationIntervals | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    repetition_basis: {
      value: cdktf.stringToHclTerraform(struct!.repetitionBasis),
      isBlock: false,
      type: "simple",
      storageClassType: "string",
    },
    monthly_repetition_basis: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisToHclTerraform, true)(struct!.monthlyRepetitionBasis),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisList",
    },
    weekly_repetition_basis: {
      value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisToHclTerraform, true)(struct!.weeklyRepetitionBasis),
      isBlock: true,
      type: "set",
      storageClassType: "OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisList",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationIntervals | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._repetitionBasis !== undefined) {
      hasAnyValues = true;
      internalValueResult.repetitionBasis = this._repetitionBasis;
    }
    if (this._monthlyRepetitionBasis?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.monthlyRepetitionBasis = this._monthlyRepetitionBasis?.internalValue;
    }
    if (this._weeklyRepetitionBasis?.internalValue !== undefined) {
      hasAnyValues = true;
      internalValueResult.weeklyRepetitionBasis = this._weeklyRepetitionBasis?.internalValue;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationIntervals | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._repetitionBasis = undefined;
      this._monthlyRepetitionBasis.internalValue = undefined;
      this._weeklyRepetitionBasis.internalValue = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._repetitionBasis = value.repetitionBasis;
      this._monthlyRepetitionBasis.internalValue = value.monthlyRepetitionBasis;
      this._weeklyRepetitionBasis.internalValue = value.weeklyRepetitionBasis;
    }
  }

  // repetition_basis - computed: false, optional: false, required: true
  private _repetitionBasis?: string; 
  public get repetitionBasis() {
    return this.getStringAttribute('repetition_basis');
  }
  public set repetitionBasis(value: string) {
    this._repetitionBasis = value;
  }
  // Temporarily expose input value. Use with caution.
  public get repetitionBasisInput() {
    return this._repetitionBasis;
  }

  // monthly_repetition_basis - computed: false, optional: true, required: false
  private _monthlyRepetitionBasis = new OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasisList(this, "monthly_repetition_basis", true);
  public get monthlyRepetitionBasis() {
    return this._monthlyRepetitionBasis;
  }
  public putMonthlyRepetitionBasis(value: OceanRightSizingRuleRecommendationApplicationIntervalsMonthlyRepetitionBasis[] | cdktf.IResolvable) {
    this._monthlyRepetitionBasis.internalValue = value;
  }
  public resetMonthlyRepetitionBasis() {
    this._monthlyRepetitionBasis.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get monthlyRepetitionBasisInput() {
    return this._monthlyRepetitionBasis.internalValue;
  }

  // weekly_repetition_basis - computed: false, optional: true, required: false
  private _weeklyRepetitionBasis = new OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasisList(this, "weekly_repetition_basis", true);
  public get weeklyRepetitionBasis() {
    return this._weeklyRepetitionBasis;
  }
  public putWeeklyRepetitionBasis(value: OceanRightSizingRuleRecommendationApplicationIntervalsWeeklyRepetitionBasis[] | cdktf.IResolvable) {
    this._weeklyRepetitionBasis.internalValue = value;
  }
  public resetWeeklyRepetitionBasis() {
    this._weeklyRepetitionBasis.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get weeklyRepetitionBasisInput() {
    return this._weeklyRepetitionBasis.internalValue;
  }
}

export class OceanRightSizingRuleRecommendationApplicationIntervalsList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationIntervals[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationIntervalsOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationIntervalsOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationMinThreshold {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#cpu_percentage OceanRightSizingRule#cpu_percentage}
  */
  readonly cpuPercentage?: number;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#memory_percentage OceanRightSizingRule#memory_percentage}
  */
  readonly memoryPercentage?: number;
}

export function oceanRightSizingRuleRecommendationApplicationMinThresholdToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationMinThreshold | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    cpu_percentage: cdktf.numberToTerraform(struct!.cpuPercentage),
    memory_percentage: cdktf.numberToTerraform(struct!.memoryPercentage),
  }
}


export function oceanRightSizingRuleRecommendationApplicationMinThresholdToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationMinThreshold | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    cpu_percentage: {
      value: cdktf.numberToHclTerraform(struct!.cpuPercentage),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
    memory_percentage: {
      value: cdktf.numberToHclTerraform(struct!.memoryPercentage),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationMinThresholdOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationMinThreshold | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._cpuPercentage !== undefined) {
      hasAnyValues = true;
      internalValueResult.cpuPercentage = this._cpuPercentage;
    }
    if (this._memoryPercentage !== undefined) {
      hasAnyValues = true;
      internalValueResult.memoryPercentage = this._memoryPercentage;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationMinThreshold | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._cpuPercentage = undefined;
      this._memoryPercentage = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._cpuPercentage = value.cpuPercentage;
      this._memoryPercentage = value.memoryPercentage;
    }
  }

  // cpu_percentage - computed: false, optional: true, required: false
  private _cpuPercentage?: number; 
  public get cpuPercentage() {
    return this.getNumberAttribute('cpu_percentage');
  }
  public set cpuPercentage(value: number) {
    this._cpuPercentage = value;
  }
  public resetCpuPercentage() {
    this._cpuPercentage = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get cpuPercentageInput() {
    return this._cpuPercentage;
  }

  // memory_percentage - computed: false, optional: true, required: false
  private _memoryPercentage?: number; 
  public get memoryPercentage() {
    return this.getNumberAttribute('memory_percentage');
  }
  public set memoryPercentage(value: number) {
    this._memoryPercentage = value;
  }
  public resetMemoryPercentage() {
    this._memoryPercentage = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get memoryPercentageInput() {
    return this._memoryPercentage;
  }
}

export class OceanRightSizingRuleRecommendationApplicationMinThresholdList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationMinThreshold[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationMinThresholdOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationMinThresholdOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}
export interface OceanRightSizingRuleRecommendationApplicationOverheadValues {
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#cpu_percentage OceanRightSizingRule#cpu_percentage}
  */
  readonly cpuPercentage?: number;
  /**
  * Docs at Terraform Registry: {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#memory_percentage OceanRightSizingRule#memory_percentage}
  */
  readonly memoryPercentage?: number;
}

export function oceanRightSizingRuleRecommendationApplicationOverheadValuesToTerraform(struct?: OceanRightSizingRuleRecommendationApplicationOverheadValues | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  return {
    cpu_percentage: cdktf.numberToTerraform(struct!.cpuPercentage),
    memory_percentage: cdktf.numberToTerraform(struct!.memoryPercentage),
  }
}


export function oceanRightSizingRuleRecommendationApplicationOverheadValuesToHclTerraform(struct?: OceanRightSizingRuleRecommendationApplicationOverheadValues | cdktf.IResolvable): any {
  if (!cdktf.canInspect(struct) || cdktf.Tokenization.isResolvable(struct)) { return struct; }
  if (cdktf.isComplexElement(struct)) {
    throw new Error("A complex element was used as configuration, this is not supported: https://cdk.tf/complex-object-as-configuration");
  }
  const attrs = {
    cpu_percentage: {
      value: cdktf.numberToHclTerraform(struct!.cpuPercentage),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
    memory_percentage: {
      value: cdktf.numberToHclTerraform(struct!.memoryPercentage),
      isBlock: false,
      type: "simple",
      storageClassType: "number",
    },
  };

  // remove undefined attributes
  return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined));
}

export class OceanRightSizingRuleRecommendationApplicationOverheadValuesOutputReference extends cdktf.ComplexObject {
  private isEmptyObject = false;
  private resolvableValue?: cdktf.IResolvable;

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param complexObjectIndex the index of this item in the list
  * @param complexObjectIsFromSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  public constructor(terraformResource: cdktf.IInterpolatingParent, terraformAttribute: string, complexObjectIndex: number, complexObjectIsFromSet: boolean) {
    super(terraformResource, terraformAttribute, complexObjectIsFromSet, complexObjectIndex);
  }

  public get internalValue(): OceanRightSizingRuleRecommendationApplicationOverheadValues | cdktf.IResolvable | undefined {
    if (this.resolvableValue) {
      return this.resolvableValue;
    }
    let hasAnyValues = this.isEmptyObject;
    const internalValueResult: any = {};
    if (this._cpuPercentage !== undefined) {
      hasAnyValues = true;
      internalValueResult.cpuPercentage = this._cpuPercentage;
    }
    if (this._memoryPercentage !== undefined) {
      hasAnyValues = true;
      internalValueResult.memoryPercentage = this._memoryPercentage;
    }
    return hasAnyValues ? internalValueResult : undefined;
  }

  public set internalValue(value: OceanRightSizingRuleRecommendationApplicationOverheadValues | cdktf.IResolvable | undefined) {
    if (value === undefined) {
      this.isEmptyObject = false;
      this.resolvableValue = undefined;
      this._cpuPercentage = undefined;
      this._memoryPercentage = undefined;
    }
    else if (cdktf.Tokenization.isResolvable(value)) {
      this.isEmptyObject = false;
      this.resolvableValue = value;
    }
    else {
      this.isEmptyObject = Object.keys(value).length === 0;
      this.resolvableValue = undefined;
      this._cpuPercentage = value.cpuPercentage;
      this._memoryPercentage = value.memoryPercentage;
    }
  }

  // cpu_percentage - computed: false, optional: true, required: false
  private _cpuPercentage?: number; 
  public get cpuPercentage() {
    return this.getNumberAttribute('cpu_percentage');
  }
  public set cpuPercentage(value: number) {
    this._cpuPercentage = value;
  }
  public resetCpuPercentage() {
    this._cpuPercentage = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get cpuPercentageInput() {
    return this._cpuPercentage;
  }

  // memory_percentage - computed: false, optional: true, required: false
  private _memoryPercentage?: number; 
  public get memoryPercentage() {
    return this.getNumberAttribute('memory_percentage');
  }
  public set memoryPercentage(value: number) {
    this._memoryPercentage = value;
  }
  public resetMemoryPercentage() {
    this._memoryPercentage = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get memoryPercentageInput() {
    return this._memoryPercentage;
  }
}

export class OceanRightSizingRuleRecommendationApplicationOverheadValuesList extends cdktf.ComplexList {
  public internalValue? : OceanRightSizingRuleRecommendationApplicationOverheadValues[] | cdktf.IResolvable

  /**
  * @param terraformResource The parent resource
  * @param terraformAttribute The attribute on the parent resource this class is referencing
  * @param wrapsSet whether the list is wrapping a set (will add tolist() to be able to access an item via an index)
  */
  constructor(protected terraformResource: cdktf.IInterpolatingParent, protected terraformAttribute: string, protected wrapsSet: boolean) {
    super(terraformResource, terraformAttribute, wrapsSet)
  }

  /**
  * @param index the index of the item to return
  */
  public get(index: number): OceanRightSizingRuleRecommendationApplicationOverheadValuesOutputReference {
    return new OceanRightSizingRuleRecommendationApplicationOverheadValuesOutputReference(this.terraformResource, this.terraformAttribute, index, this.wrapsSet);
  }
}

/**
* Represents a {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule spotinst_ocean_right_sizing_rule}
*/
export class OceanRightSizingRule extends cdktf.TerraformResource {

  // =================
  // STATIC PROPERTIES
  // =================
  public static readonly tfResourceType = "spotinst_ocean_right_sizing_rule";

  // ==============
  // STATIC Methods
  // ==============
  /**
  * Generates CDKTF code for importing a OceanRightSizingRule resource upon running "cdktf plan <stack-name>"
  * @param scope The scope in which to define this construct
  * @param importToId The construct id used in the generated config for the OceanRightSizingRule to import
  * @param importFromId The id of the existing OceanRightSizingRule that should be imported. Refer to the {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule#import import section} in the documentation of this resource for the id to use
  * @param provider? Optional instance of the provider where the OceanRightSizingRule to import is found
  */
  public static generateConfigForImport(scope: Construct, importToId: string, importFromId: string, provider?: cdktf.TerraformProvider) {
        return new cdktf.ImportableResource(scope, importToId, { terraformResourceType: "spotinst_ocean_right_sizing_rule", importId: importFromId, provider });
      }

  // ===========
  // INITIALIZER
  // ===========

  /**
  * Create a new {@link https://registry.terraform.io/providers/spotinst/spotinst/1.179.0/docs/resources/ocean_right_sizing_rule spotinst_ocean_right_sizing_rule} Resource
  *
  * @param scope The scope in which to define this construct
  * @param id The scoped construct ID. Must be unique amongst siblings in the same scope
  * @param options OceanRightSizingRuleConfig
  */
  public constructor(scope: Construct, id: string, config: OceanRightSizingRuleConfig) {
    super(scope, id, {
      terraformResourceType: 'spotinst_ocean_right_sizing_rule',
      terraformGeneratorMetadata: {
        providerName: 'spotinst',
        providerVersion: '1.179.0',
        providerVersionConstraint: '~> 1.0'
      },
      provider: config.provider,
      dependsOn: config.dependsOn,
      count: config.count,
      lifecycle: config.lifecycle,
      provisioners: config.provisioners,
      connection: config.connection,
      forEach: config.forEach
    });
    this._id = config.id;
    this._oceanId = config.oceanId;
    this._restartPods = config.restartPods;
    this._ruleName = config.ruleName;
    this._attachWorkloads.internalValue = config.attachWorkloads;
    this._detachWorkloads.internalValue = config.detachWorkloads;
    this._recommendationApplicationBoundaries.internalValue = config.recommendationApplicationBoundaries;
    this._recommendationApplicationIntervals.internalValue = config.recommendationApplicationIntervals;
    this._recommendationApplicationMinThreshold.internalValue = config.recommendationApplicationMinThreshold;
    this._recommendationApplicationOverheadValues.internalValue = config.recommendationApplicationOverheadValues;
  }

  // ==========
  // ATTRIBUTES
  // ==========

  // id - computed: true, optional: true, required: false
  private _id?: string; 
  public get id() {
    return this.getStringAttribute('id');
  }
  public set id(value: string) {
    this._id = value;
  }
  public resetId() {
    this._id = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get idInput() {
    return this._id;
  }

  // ocean_id - computed: false, optional: true, required: false
  private _oceanId?: string; 
  public get oceanId() {
    return this.getStringAttribute('ocean_id');
  }
  public set oceanId(value: string) {
    this._oceanId = value;
  }
  public resetOceanId() {
    this._oceanId = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get oceanIdInput() {
    return this._oceanId;
  }

  // restart_pods - computed: false, optional: true, required: false
  private _restartPods?: boolean | cdktf.IResolvable; 
  public get restartPods() {
    return this.getBooleanAttribute('restart_pods');
  }
  public set restartPods(value: boolean | cdktf.IResolvable) {
    this._restartPods = value;
  }
  public resetRestartPods() {
    this._restartPods = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get restartPodsInput() {
    return this._restartPods;
  }

  // rule_name - computed: false, optional: false, required: true
  private _ruleName?: string; 
  public get ruleName() {
    return this.getStringAttribute('rule_name');
  }
  public set ruleName(value: string) {
    this._ruleName = value;
  }
  // Temporarily expose input value. Use with caution.
  public get ruleNameInput() {
    return this._ruleName;
  }

  // attach_workloads - computed: false, optional: true, required: false
  private _attachWorkloads = new OceanRightSizingRuleAttachWorkloadsList(this, "attach_workloads", true);
  public get attachWorkloads() {
    return this._attachWorkloads;
  }
  public putAttachWorkloads(value: OceanRightSizingRuleAttachWorkloads[] | cdktf.IResolvable) {
    this._attachWorkloads.internalValue = value;
  }
  public resetAttachWorkloads() {
    this._attachWorkloads.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get attachWorkloadsInput() {
    return this._attachWorkloads.internalValue;
  }

  // detach_workloads - computed: false, optional: true, required: false
  private _detachWorkloads = new OceanRightSizingRuleDetachWorkloadsList(this, "detach_workloads", true);
  public get detachWorkloads() {
    return this._detachWorkloads;
  }
  public putDetachWorkloads(value: OceanRightSizingRuleDetachWorkloads[] | cdktf.IResolvable) {
    this._detachWorkloads.internalValue = value;
  }
  public resetDetachWorkloads() {
    this._detachWorkloads.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get detachWorkloadsInput() {
    return this._detachWorkloads.internalValue;
  }

  // recommendation_application_boundaries - computed: false, optional: true, required: false
  private _recommendationApplicationBoundaries = new OceanRightSizingRuleRecommendationApplicationBoundariesList(this, "recommendation_application_boundaries", false);
  public get recommendationApplicationBoundaries() {
    return this._recommendationApplicationBoundaries;
  }
  public putRecommendationApplicationBoundaries(value: OceanRightSizingRuleRecommendationApplicationBoundaries[] | cdktf.IResolvable) {
    this._recommendationApplicationBoundaries.internalValue = value;
  }
  public resetRecommendationApplicationBoundaries() {
    this._recommendationApplicationBoundaries.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get recommendationApplicationBoundariesInput() {
    return this._recommendationApplicationBoundaries.internalValue;
  }

  // recommendation_application_intervals - computed: false, optional: false, required: true
  private _recommendationApplicationIntervals = new OceanRightSizingRuleRecommendationApplicationIntervalsList(this, "recommendation_application_intervals", true);
  public get recommendationApplicationIntervals() {
    return this._recommendationApplicationIntervals;
  }
  public putRecommendationApplicationIntervals(value: OceanRightSizingRuleRecommendationApplicationIntervals[] | cdktf.IResolvable) {
    this._recommendationApplicationIntervals.internalValue = value;
  }
  // Temporarily expose input value. Use with caution.
  public get recommendationApplicationIntervalsInput() {
    return this._recommendationApplicationIntervals.internalValue;
  }

  // recommendation_application_min_threshold - computed: false, optional: true, required: false
  private _recommendationApplicationMinThreshold = new OceanRightSizingRuleRecommendationApplicationMinThresholdList(this, "recommendation_application_min_threshold", true);
  public get recommendationApplicationMinThreshold() {
    return this._recommendationApplicationMinThreshold;
  }
  public putRecommendationApplicationMinThreshold(value: OceanRightSizingRuleRecommendationApplicationMinThreshold[] | cdktf.IResolvable) {
    this._recommendationApplicationMinThreshold.internalValue = value;
  }
  public resetRecommendationApplicationMinThreshold() {
    this._recommendationApplicationMinThreshold.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get recommendationApplicationMinThresholdInput() {
    return this._recommendationApplicationMinThreshold.internalValue;
  }

  // recommendation_application_overhead_values - computed: false, optional: true, required: false
  private _recommendationApplicationOverheadValues = new OceanRightSizingRuleRecommendationApplicationOverheadValuesList(this, "recommendation_application_overhead_values", true);
  public get recommendationApplicationOverheadValues() {
    return this._recommendationApplicationOverheadValues;
  }
  public putRecommendationApplicationOverheadValues(value: OceanRightSizingRuleRecommendationApplicationOverheadValues[] | cdktf.IResolvable) {
    this._recommendationApplicationOverheadValues.internalValue = value;
  }
  public resetRecommendationApplicationOverheadValues() {
    this._recommendationApplicationOverheadValues.internalValue = undefined;
  }
  // Temporarily expose input value. Use with caution.
  public get recommendationApplicationOverheadValuesInput() {
    return this._recommendationApplicationOverheadValues.internalValue;
  }

  // =========
  // SYNTHESIS
  // =========

  protected synthesizeAttributes(): { [name: string]: any } {
    return {
      id: cdktf.stringToTerraform(this._id),
      ocean_id: cdktf.stringToTerraform(this._oceanId),
      restart_pods: cdktf.booleanToTerraform(this._restartPods),
      rule_name: cdktf.stringToTerraform(this._ruleName),
      attach_workloads: cdktf.listMapper(oceanRightSizingRuleAttachWorkloadsToTerraform, true)(this._attachWorkloads.internalValue),
      detach_workloads: cdktf.listMapper(oceanRightSizingRuleDetachWorkloadsToTerraform, true)(this._detachWorkloads.internalValue),
      recommendation_application_boundaries: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationBoundariesToTerraform, true)(this._recommendationApplicationBoundaries.internalValue),
      recommendation_application_intervals: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationIntervalsToTerraform, true)(this._recommendationApplicationIntervals.internalValue),
      recommendation_application_min_threshold: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationMinThresholdToTerraform, true)(this._recommendationApplicationMinThreshold.internalValue),
      recommendation_application_overhead_values: cdktf.listMapper(oceanRightSizingRuleRecommendationApplicationOverheadValuesToTerraform, true)(this._recommendationApplicationOverheadValues.internalValue),
    };
  }

  protected synthesizeHclAttributes(): { [name: string]: any } {
    const attrs = {
      id: {
        value: cdktf.stringToHclTerraform(this._id),
        isBlock: false,
        type: "simple",
        storageClassType: "string",
      },
      ocean_id: {
        value: cdktf.stringToHclTerraform(this._oceanId),
        isBlock: false,
        type: "simple",
        storageClassType: "string",
      },
      restart_pods: {
        value: cdktf.booleanToHclTerraform(this._restartPods),
        isBlock: false,
        type: "simple",
        storageClassType: "boolean",
      },
      rule_name: {
        value: cdktf.stringToHclTerraform(this._ruleName),
        isBlock: false,
        type: "simple",
        storageClassType: "string",
      },
      attach_workloads: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleAttachWorkloadsToHclTerraform, true)(this._attachWorkloads.internalValue),
        isBlock: true,
        type: "set",
        storageClassType: "OceanRightSizingRuleAttachWorkloadsList",
      },
      detach_workloads: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleDetachWorkloadsToHclTerraform, true)(this._detachWorkloads.internalValue),
        isBlock: true,
        type: "set",
        storageClassType: "OceanRightSizingRuleDetachWorkloadsList",
      },
      recommendation_application_boundaries: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationBoundariesToHclTerraform, true)(this._recommendationApplicationBoundaries.internalValue),
        isBlock: true,
        type: "list",
        storageClassType: "OceanRightSizingRuleRecommendationApplicationBoundariesList",
      },
      recommendation_application_intervals: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationIntervalsToHclTerraform, true)(this._recommendationApplicationIntervals.internalValue),
        isBlock: true,
        type: "set",
        storageClassType: "OceanRightSizingRuleRecommendationApplicationIntervalsList",
      },
      recommendation_application_min_threshold: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationMinThresholdToHclTerraform, true)(this._recommendationApplicationMinThreshold.internalValue),
        isBlock: true,
        type: "set",
        storageClassType: "OceanRightSizingRuleRecommendationApplicationMinThresholdList",
      },
      recommendation_application_overhead_values: {
        value: cdktf.listMapperHcl(oceanRightSizingRuleRecommendationApplicationOverheadValuesToHclTerraform, true)(this._recommendationApplicationOverheadValues.internalValue),
        isBlock: true,
        type: "set",
        storageClassType: "OceanRightSizingRuleRecommendationApplicationOverheadValuesList",
      },
    };

    // remove undefined attributes
    return Object.fromEntries(Object.entries(attrs).filter(([_, value]) => value !== undefined && value.value !== undefined ))
  }
}