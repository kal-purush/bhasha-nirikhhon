import { all, ComponentResourceOptions, Output } from "@pulumi/pulumi";
import { RandomPassword } from "@pulumi/random";
import { Component } from "../component";
import { FunctionArgs, Function } from ".";
import { functionBuilder } from "./helpers/function-builder";
import { Input } from "../input";

export interface OpenControlArgs {
  server: Input<string | FunctionArgs>;
}

export class OpenControl extends Component {
  private readonly _server: Output<Function>;
  private readonly _key: Output<string>;

  constructor(
    name: string,
    args: OpenControlArgs,
    opts?: ComponentResourceOptions,
  ) {
    super(__pulumiType, name, args, opts);
    const self = this;

    const key = createKey();
    const server = createServer();

    this._server = server;
    this._key = key;
    registerOutputs();

    function registerOutputs() {
      self.registerOutputs({
        _hint: self.url,
      });
    }

    function createKey() {
      return new RandomPassword(
        `${name}Key`,
        {
          length: 16,
          special: false,
        },
        { parent: self },
      ).result;
    }

    function createServer() {
      return functionBuilder(
        `${name}Server`,
        args.server,
        {
          link: [],
          environment: {
            OPENCONTROL_KEY: key,
          },
          url: true,
          _skipHint: true,
        },
        (args) => {
          args.url = {
            cors: false,
          };
        },
        { parent: self },
      ).apply((v) => v.getFunction());
    }
  }

  /**
   * The URL of the OpenControl server.
   */
  public get url() {
    return all([this._server.url, this._key]).apply(
      ([url, key]) => `${url}${key}`,
    );
  }

  /**
   * The underlying [resources](/docs/components/#nodes) this component creates.
   */
  public get nodes() {
    return {
      /**
       * The Function component for the server.
       */
      server: this._server,
    };
  }
}

const __pulumiType = "sst:aws:OpenControl";
// @ts-expect-error
OpenControl.__pulumiType = __pulumiType;