from brownie import config, network, CanariLottery
from scripts.helpful_scripts import get_account, get_contract, fund_with_link
import time


def main():
    deploy_lottery()
    start_lottery()
    enter_lottery()
    end_lottery()


def deploy_lottery():
    print("**** KEY HASH", config["networks"][network.show_active()]["vrf_key_hash"])
    account = get_account()
    lottery = CanariLottery.deploy(
        get_contract("eth_usd_price_feed").address,
        get_contract("vrf_coordinator").address,
        get_contract("vrf_link_token").address,
        config["networks"][network.show_active()]["vrf_key_hash"],
        config["networks"][network.show_active()]["vrf_fee"],
        {"from": account},
        publish_source=config["networks"][network.show_active()].get("verify", False),
    )
    print("Canary Lottery has been deployed at address", lottery.address)
    return lottery


def start_lottery():
    account = get_account()
    lottery = CanariLottery[-1]
    starting_tx = lottery.startLottery({"from": account})
    starting_tx.wait(1)
    print("The Canary Lottery has started")


def enter_lottery():
    account = get_account()
    lottery = CanariLottery[-1]
    value = lottery.getEntranceFee() + 100000000
    lottery.enter({"from": account, "value": value})
    print(
        account, " Has entered the Lottery and has paid ", (value / (10 ** 18)), "ether"
    )


def end_lottery():
    account = get_account()
    lottery = CanariLottery[-1]
    tx_fund = fund_with_link(lottery.address)
    tx_fund.wait(1)
    ending_tx = lottery.endLottery({"from": account})
    ending_tx.wait(1)
    time.sleep(180)
    print(f"{lottery.winner()} is the new winner")