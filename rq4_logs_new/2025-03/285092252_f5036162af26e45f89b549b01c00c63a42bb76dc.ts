import { CustomResourceOptions, Input, dynamic } from "@pulumi/pulumi";
import { rpc } from "../../rpc/rpc.js";
import { FunctionEnvironmentArgs } from "../function.js";

export interface FunctionEnvironmentUpdateInputs {
  functionName: Input<string>;
  environment: Input<FunctionEnvironmentArgs>;
  region: Input<string>;
}

export class FunctionEnvironmentUpdate extends dynamic.Resource {
  constructor(
    name: string,
    args: FunctionEnvironmentUpdateInputs,
    opts?: CustomResourceOptions,
  ) {
    super(
      new rpc.Provider("Aws.FunctionEnvironmentUpdate"),
      `${name}.sst.aws.FunctionEnvironmentUpdate`,
      args,
      opts,
    );
  }
}