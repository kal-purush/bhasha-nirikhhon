import { ChildProcessWithoutNullStreams } from "child_process";
import * as fs from "fs";
import path from "path";
import { parseAbi, parseEther } from "viem";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import {
    AnvilTestEnvironment,
    BASE_ETH_AMOUNT,
    BASE_GRT_AMOUNT,
    E2E_SCENARIO_SETUP_TIMEOUT,
    EPOCH_MANAGER_GOVERNOR_ADDRESS,
    fundAccountWithGrt,
    GRT_HOLDER,
    setupAnvilTestEnvironment,
} from "../../utils/e2e-scaffold/index.js";
import { killAgent } from "../../utils/e2e-scaffold/spawnAgent.js";

describe("E2E Setup", () => {
    const tmpConfigDir = path.join(__dirname, "/tmp");
    const tmpConfigFile = path.join(tmpConfigDir, "/config.yml");

    let agent: ChildProcessWithoutNullStreams | undefined;
    let testEnv: AnvilTestEnvironment;

    beforeAll(async () => {
        // Setup the test environment with dynamic port allocation
        testEnv = await setupAnvilTestEnvironment(__dirname);
    }, E2E_SCENARIO_SETUP_TIMEOUT);

    afterAll(async () => {
        // Clean up the test environment
        await testEnv.cleanup();
    });

    afterEach(async () => {
        await testEnv.anvilClient.revert({ id: testEnv.snapshotId });

        if (fs.existsSync(tmpConfigFile) && agent) {
            await killAgent({
                process: agent,
                configPath: tmpConfigFile,
            });
        }
    });

    it("verify all protocol contracts have code", async () => {
        for (const [name, address] of Object.entries(testEnv.protocolContracts)) {
            if (name !== "arbitrator" && name !== "council") {
                const code = await testEnv.anvilClient.getCode({ address });
                expect(code, `Contract ${name} at ${address} should have code`).not.toBe("0x");
                expect(code, `Contract ${name} at ${address} should have code`).not.toBe(null);
            }
        }
    });

    it("verify all accounts have the expected ETH and GRT balances", async () => {
        for (const [index, accountData] of testEnv.withoutStakeAccounts.entries()) {
            // Check ETH balance
            const ethBalance = await testEnv.anvilClient.getBalance({
                address: accountData.account.address,
            });
            expect(ethBalance, `Account ${index} should have ${BASE_ETH_AMOUNT} ETH`).toBe(
                parseEther(BASE_ETH_AMOUNT.toString()),
            );

            // Check GRT balance
            const grtBalance = await testEnv.anvilClient.readContract({
                address: testEnv.protocolContracts.grt,
                abi: parseAbi(["function balanceOf(address) returns (uint256)"]),
                functionName: "balanceOf",
                args: [accountData.account.address],
            });
            expect(grtBalance, `Account ${index} should have ${BASE_GRT_AMOUNT} GRT`).toBe(
                parseEther(BASE_GRT_AMOUNT.toString()),
            );
        }

        for (const [index, accountData] of testEnv.withStakeAccounts.entries()) {
            // Check ETH balance
            const ethBalance = await testEnv.anvilClient.getBalance({
                address: accountData.account.address,
            });
            expect(
                ethBalance,
                `Account ${index} should have slightly less than ${BASE_ETH_AMOUNT} ETH due to staking gas costs`,
            ).toBeLessThan(parseEther(BASE_ETH_AMOUNT.toString()));
            expect(ethBalance, `Account ${index} should have more than 0 ETH`).toBeGreaterThan(0n);

            // Check GRT balance
            const grtBalance = await testEnv.anvilClient.readContract({
                address: testEnv.protocolContracts.grt,
                abi: parseAbi(["function balanceOf(address) returns (uint256)"]),
                functionName: "balanceOf",
                args: [accountData.account.address],
            });
            expect(
                grtBalance,
                `Account ${index} should have ${BASE_GRT_AMOUNT} GRT due to staking`,
            ).toBe(0n);
        }
    });

    it("verify EPOCH_MANAGER_GOVERNOR_ADDRESS and GRT_HOLDER have ETH balance", async () => {
        const governorEthBalance = await testEnv.anvilClient.getBalance({
            address: EPOCH_MANAGER_GOVERNOR_ADDRESS,
        });
        expect(
            governorEthBalance,
            `EPOCH_MANAGER_GOVERNOR_ADDRESS should have ETH`,
        ).toBeGreaterThan(0n);

        const holderEthBalance = await testEnv.anvilClient.getBalance({
            address: GRT_HOLDER,
        });
        expect(holderEthBalance, `GRT_HOLDER should have ETH`).toBeGreaterThan(0n);

        const holderGrtBalance = await testEnv.anvilClient.readContract({
            address: testEnv.protocolContracts.grt,
            abi: parseAbi(["function balanceOf(address) returns (uint256)"]),
            functionName: "balanceOf",
            args: [GRT_HOLDER],
        });
        expect(holderGrtBalance, `GRT_HOLDER should have GRT`).toBeGreaterThan(0n);
    });

    it("doesn't mix state between tests", async () => {
        await fundAccountWithGrt(testEnv.withoutStakeAccounts[0]!.account, testEnv.anvilClient, {
            holderAddress: GRT_HOLDER,
            contractAddress: testEnv.protocolContracts.grt,
            fundAmount: parseEther(BASE_GRT_AMOUNT.toString()),
        });
        const balance = await testEnv.anvilClient.readContract({
            address: testEnv.protocolContracts.grt,
            abi: parseAbi(["function balanceOf(address) returns (uint256)"]),
            functionName: "balanceOf",
            args: [testEnv.withoutStakeAccounts[0]!.account.address],
        });

        // this is the balance from the beforeEach setup + the balance from the fundAccount
        expect(balance).toBe(parseEther((BASE_GRT_AMOUNT * 2n).toString()));
    });
});