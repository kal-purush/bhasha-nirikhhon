import { Project } from "@real-wagmi/sdk";
import getProtocolLogo from "../getProtocolLogo";
import { expect, it, describe } from "vitest";
import { WagmiIconProtocol } from "../../components";

describe("getProtocolLogo", () => {
  it("should return logo", () => {
    const logo = getProtocolLogo(Project.WAGMI);
    expect(logo).toBe(WagmiIconProtocol);
  });
});