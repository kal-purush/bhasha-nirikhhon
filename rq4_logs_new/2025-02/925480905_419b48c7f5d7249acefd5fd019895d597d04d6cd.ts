import {
  Clarinet,
  Tx,
  Chain,
  Account,
  types
} from 'https://deno.land/x/clarinet@v1.0.0/index.ts';
import { assertEquals } from 'https://deno.land/std@0.90.0/testing/asserts.ts';

Clarinet.test({
  name: "Ensure can create new task",
  async fn(chain: Chain, accounts: Map<string, Account>) {
    const wallet_1 = accounts.get("wallet_1")!;
    
    let block = chain.mineBlock([
      Tx.contractCall("pulsebeam", "create-task", [
        types.ascii("Test Task"),
        types.uint(1000),
        types.bool(true),
        types.bool(false)
      ], wallet_1.address)
    ]);
    
    assertEquals(block.receipts.length, 1);
    assertEquals(block.height, 2);
    block.receipts[0].result.expectOk().expectUint(1);
  },
});

Clarinet.test({
  name: "Ensure can complete task",
  async fn(chain: Chain, accounts: Map<string, Account>) {
    const wallet_1 = accounts.get("wallet_1")!;
    
    let block = chain.mineBlock([
      Tx.contractCall("pulsebeam", "create-task", [
        types.ascii("Test Task"),
        types.uint(1000),
        types.bool(true),
        types.bool(false)
      ], wallet_1.address),
      Tx.contractCall("pulsebeam", "complete-task", [
        types.uint(1)
      ], wallet_1.address)
    ]);
    
    assertEquals(block.receipts.length, 2);
    assertEquals(block.height, 2);
    block.receipts[1].result.expectOk().expectBool(true);
  },
});

Clarinet.test({
  name: "Ensure can update notification preferences",
  async fn(chain: Chain, accounts: Map<string, Account>) {
    const wallet_1 = accounts.get("wallet_1")!;
    
    let block = chain.mineBlock([
      Tx.contractCall("pulsebeam", "create-task", [
        types.ascii("Test Task"),
        types.uint(1000),
        types.bool(true),
        types.bool(false)
      ], wallet_1.address),
      Tx.contractCall("pulsebeam", "update-notifications", [
        types.uint(1),
        types.bool(false),
        types.bool(true)
      ], wallet_1.address)
    ]);
    
    assertEquals(block.receipts.length, 2);
    assertEquals(block.height, 2);
    block.receipts[1].result.expectOk().expectBool(true);
  },
});