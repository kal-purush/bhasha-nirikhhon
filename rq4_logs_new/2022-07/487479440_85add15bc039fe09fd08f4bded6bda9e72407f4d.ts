import { expect } from "chai";
import { ethers } from "hardhat";
import { BigNumber } from "ethers";
import type { SignerWithAddress } from "@nomiclabs/hardhat-ethers/signers";
import {SpaceCoinICO, SpaceCoinICO__factory} from "../typechain/";

describe("SpaceCoin ICO", function () {
    let spaceCoinICO: SpaceCoinICO;
    let spaceCoinICOFactory: SpaceCoinICO__factory;
    let totalBalance: BigNumber;
    let owner: SignerWithAddress;
    let treasury: SignerWithAddress;
    let alice: SignerWithAddress;
    let bob: SignerWithAddress;
    let addrs: SignerWithAddress[];
    let whitelist: string[];
  
    this.beforeEach(async function() {
      [owner, treasury, alice, bob, ...addrs] = await ethers.getSigners();
      
      whitelist = [addrs[0].address, addrs[1].address, addrs[2].address, addrs[3].address, addrs[4].address, 
                  addrs[5].address, addrs[6].address, addrs[7].address, addrs[8].address, addrs[9].address, 
                  addrs[10].address, addrs[11].address, addrs[12].address, addrs[13].address, addrs[14].address, 
                  addrs[15].address];
  
      spaceCoinICOFactory = await ethers.getContractFactory("SpaceCoinICO");
      spaceCoinICO = (await spaceCoinICOFactory.connect(owner).deploy(treasury.address, whitelist)) as SpaceCoinICO;
      await spaceCoinICO.deployed();
      console.log("contract address: ", spaceCoinICO.address);
  
      totalBalance = await spaceCoinICO.connect(owner).balanceOf(spaceCoinICO.address);
      console.log("total contract balance: ", totalBalance);
    });

    describe("toggle tax flag", function () {
        it("toggle tax flag, by owner", async function() {
            let canDeductTax: boolean = await spaceCoinICO.canDeductTax();
            await expect(spaceCoinICO.connect(owner).toggleTaxFlag())
            .to.emit(spaceCoinICO, "ToggleTaxFlag").withArgs(owner.address, !canDeductTax);

        });
        it("toggle tax flag, by someone other than owner", async function() {
            await expect(spaceCoinICO.connect(alice).toggleTaxFlag()).to.be.revertedWith("NOT_OWNER");
        });
    });

    describe("toggle ico status", function () {
        it("toggle ico status, by owner", async function() {
            let icoStatus: boolean = await spaceCoinICO.pauseICO();
            await expect(spaceCoinICO.connect(owner).toggleICOStatus())
            .to.emit(spaceCoinICO, "ToggleICOStatus").withArgs(owner.address, !icoStatus);
        });
        it("toggle ico status, by someone other than owner", async function() {
        await expect(spaceCoinICO.connect(alice).toggleICOStatus()).to.be.revertedWith("NOT_OWNER");
        });
    });

    describe("change ico phase by user", function () {
        it("change ico phase from SEED => GENERAL => OPEN [forward direction], by owner", async function() {
            await expect(spaceCoinICO.connect(owner).changeICOStage(1))
            .to.emit(spaceCoinICO, "ChangeICOStage").withArgs(owner.address, 0, 1);
            await expect(spaceCoinICO.connect(owner).changeICOStage(2))
            .to.emit(spaceCoinICO, "ChangeICOStage").withArgs(owner.address, 1, 2);
            
            await expect(spaceCoinICO.connect(owner).changeICOStage(3)).to.be.revertedWith("NO_MORE_PHASE");
        });
        it("change ico phase, by someone other than owner", async function() {
            await expect(spaceCoinICO.connect(alice).changeICOStage(0)).to.be.revertedWith("NOT_OWNER");
        });
    });

    describe("transfer token", function () {
        it("transfer during SEED phase", async function() {
            await expect(spaceCoinICO.connect(owner).transfer(alice.address, 50))
            .to.be.revertedWith("NOT_READY_TO_TRANSFER");
        });
        
        it("transfer during GENERAL phase", async function() {
            // move to GENERAL phase
            await spaceCoinICO.connect(owner).changeICOStage(1);
            await expect(spaceCoinICO.connect(owner).transfer(alice.address, 50))
            .to.be.revertedWith("NOT_READY_TO_TRANSFER");
        });

        describe("transfer during OPEN phase, no TAX", function () {
            this.beforeEach(async function() {
                // move to GENERAL phase
                await spaceCoinICO.connect(owner).changeICOStage(1);

                // move to OPEN phase
                await spaceCoinICO.connect(owner).changeICOStage(2);
            });
            it("transfer by someone who has balance", async function() {
                await spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("1")});
                expect(await spaceCoinICO.connect(owner).balanceOf(alice.address)).to.equal(ethers.utils.parseEther("5"));
                await spaceCoinICO.connect(alice).transfer(bob.address, 2);
                expect(await spaceCoinICO.connect(owner).balanceOf(alice.address)).to.equal("4999999999999999998");
                expect(await spaceCoinICO.connect(owner).balanceOf(bob.address)).to.equal("2");
            });
            it("transfer by someone who doesn't have balance", async function() {
                await expect(spaceCoinICO.connect(bob).transfer(alice.address, 50)).to.be.revertedWith("NOT_ENOUGH_TOKEN");
            });
        });

        describe("transfer during OPEN phase, with TAX deduction", function () {
            this.beforeEach(async function() {
                // move to GENERAL phase
                await spaceCoinICO.connect(owner).changeICOStage(1);

                // move to OPEN phase
                await spaceCoinICO.connect(owner).changeICOStage(2);
                await spaceCoinICO.connect(owner).toggleTaxFlag();
            });
            it("transfer by someone who has balance", async function() {
                // before transfer
                await spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("200")});
                expect(await spaceCoinICO.connect(owner).balanceOf(alice.address)).to.equal(ethers.utils.parseEther("980"));
                expect(await spaceCoinICO.connect(owner).balanceOf(bob.address)).to.equal(ethers.utils.parseEther("0"));
                expect(await spaceCoinICO.connect(owner).balanceOf(treasury.address)).to.equal(ethers.utils.parseEther("20"));

                await spaceCoinICO.connect(alice).transfer(bob.address, 100);

                // after transfer
                expect(await spaceCoinICO.connect(owner).balanceOf(alice.address)).to.equal("979999999999999999900");
                expect(await spaceCoinICO.connect(owner).balanceOf(bob.address)).to.equal("98");
                expect(await spaceCoinICO.connect(owner).balanceOf(treasury.address)).to.equal("20000000000000000002");
            });
            it("transfer by someone who doesn't have balance", async function() {
                await expect(spaceCoinICO.connect(bob).transfer(alice.address, 100)).to.be.revertedWith("NOT_ENOUGH_TOKEN");
            });
        });
    });

    describe("invest in spc", function () {
        describe("invest when on hold", function () {
            this.beforeEach(async function() {
                // put ICO on hold
                await spaceCoinICO.connect(owner).toggleICOStatus();
            });
            it("invest by whitelisted person", async function() {
                await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("1")}))
                .to.be.revertedWith("NOT_READY_FOR_SELL");
            });
            it("invest by not whitelisted person", async function() {
                await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("1")}))
                .to.be.revertedWith("NOT_READY_FOR_SELL");
            });
        });
        describe("invest when not on hold", function () {
            describe("invest during SEED phase", function () {
                describe("invest by a whitelisted person", function () {
                    it("via investInSPC() function", async function() {
                        await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("1500")}))
                        .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[0].address, ethers.utils.parseEther("1500"), 0);
                    });
                    it("via receive() function", async function() {
                        const tx = {
                        to: spaceCoinICO.address,
                        value: ethers.utils.parseEther("10")
                        }
                        await expect(await addrs[1].sendTransaction(tx))
                        .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[1].address, ethers.utils.parseEther("10"), 0);
                    });
                    it("a person who reached INDIV_LIMIT_SEED", async function() {
                        await spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("1500")});
                        await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("1500")}))
                        .to.be.revertedWith("SEED_CAP_REACHED");
                    });
                });
                describe("validate token balance during SEED phase", function () {
                    it("invest and validate balance", async function() {
                        await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("1")}))
                        .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[0].address, ethers.utils.parseEther("1"), 0);
                        expect(await spaceCoinICO.balanceOf(addrs[0].address)).to.equal(ethers.utils.parseEther("0"));
                    });
                });
                describe("invest by not a whitelisted person", function () {
                    it("via investInSPC() function", async function() {
                        await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("10")}))
                        .to.be.revertedWith("NOT_WHITELISTED");
                    });
                    it("via receive() function", async function() {
                        const tx = {
                        to: spaceCoinICO.address,
                        value: ethers.utils.parseEther("10"),
                        gasLimit: 500000
                        }
                        await expect(bob.sendTransaction(tx)).to.be.revertedWith("NOT_WHITELISTED");
                    });
                });
            });
            describe("invest during GENERAL phase", function () {
                this.beforeEach(async function() {
                    await spaceCoinICO.connect(addrs[1]).investInSPC({value: ethers.utils.parseEther("1500")});
                    // move to GENERAL phase
                    await spaceCoinICO.connect(owner).changeICOStage(1);
                });
                it("invest by a whitelisted person", async function() {
                    await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("10")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[0].address, ethers.utils.parseEther("10"), 0);
                });
                it("invest by not a whitelisted person", async function() {
                    await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("1000")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(alice.address, ethers.utils.parseEther("1000"), 0);
                });
                it("a person who reached INDIV_LIMIT_GENERAL", async function() {
                    await spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("1000")});
                    await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("100")}))
                    .to.be.revertedWith("GENERAL_CAP_REACHED");
                });
                it("a person who reached INDIV_LIMIT_SEED and then trying again in GENERAL stage", async function() {
                    await expect(spaceCoinICO.connect(addrs[1]).investInSPC({value: ethers.utils.parseEther("1")}))
                    .to.be.revertedWith("GENERAL_CAP_REACHED");
                });
                it("validate token balance during GENERAL phase", async function() {
                    await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("1")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(alice.address, ethers.utils.parseEther("1"), 0);
                    expect(await spaceCoinICO.balanceOf(alice.address)).to.equal(ethers.utils.parseEther("0"));
                });
            });
            describe("invest during OPEN phase", function () {
                this.beforeEach(async function() {
                    await spaceCoinICO.connect(addrs[10]).investInSPC({value: ethers.utils.parseEther("1500")});

                    // move to GENERAL phase
                    await spaceCoinICO.connect(owner).changeICOStage(1);
                    await spaceCoinICO.connect(bob).investInSPC({value: ethers.utils.parseEther("1000")});

                    // move to OPEN phase
                    await spaceCoinICO.connect(owner).changeICOStage(2);
                });
                it("invest by a whitelisted person without any restriction", async function() {
                    await expect(spaceCoinICO.connect(addrs[10]).investInSPC({value: ethers.utils.parseEther("2000")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC")
                    .withArgs(addrs[10].address, ethers.utils.parseEther("2000"), ethers.utils.parseEther("17500"));
                });
                it("invest by a non-whitelisted person without any restriction", async function() {
                    await expect(spaceCoinICO.connect(bob).investInSPC({value: ethers.utils.parseEther("2000")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC")
                    .withArgs(bob.address, ethers.utils.parseEther("2000"), ethers.utils.parseEther("15000"));
                });
                it("invest after GOAL reached", async function() {
                    await spaceCoinICO.connect(addrs[3]).investInSPC({value: ethers.utils.parseEther("9999")});
                    await spaceCoinICO.connect(addrs[4]).investInSPC({value: ethers.utils.parseEther("9999")});
                    await expect(spaceCoinICO.connect(addrs[5]).investInSPC({value: ethers.utils.parseEther("9999")}))
                    .to.be.revertedWith("GOAL_REACHED");
                });
                it("validate token balance during OPEN phase", async function() {
                    await expect(spaceCoinICO.connect(addrs[6]).investInSPC({value: ethers.utils.parseEther("1")}))
                    .to.emit(spaceCoinICO, "InvestedInSPC")
                    .withArgs(addrs[6].address, ethers.utils.parseEther("1"), ethers.utils.parseEther("5"));
                    expect(await spaceCoinICO.balanceOf(addrs[6].address)).to.equal(ethers.utils.parseEther("5"));
                });
            });
        });
    });

    describe("claim/ release spc", function () {
        describe("invest and claim during SEED phase", function () {
            it("via investInSPC() function", async function() {
                await expect(spaceCoinICO.connect(addrs[7]).investInSPC({value: ethers.utils.parseEther("1500")}))
                .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[7].address, ethers.utils.parseEther("1500"), 0);
                await expect(spaceCoinICO.connect(addrs[7]).claimToken(addrs[7].address))
                .to.be.revertedWith("NOT_READY_TO_CLAIM");
            });
        });
        describe("invest and claim during GENERAL phase", function () {
            this.beforeEach(async function() {
                // move to GENERAL phase
                await spaceCoinICO.connect(owner).changeICOStage(1);
            });
            it("invest & claim by a whitelisted person", async function() {
                await expect(spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("10")}))
                .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(addrs[0].address, ethers.utils.parseEther("10"), 0);
            });
            it("invest & claim by not a whitelisted person", async function() {
                await expect(spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("200")}))
                .to.emit(spaceCoinICO, "InvestedInSPC").withArgs(alice.address, ethers.utils.parseEther("200"), 0);
                await expect(spaceCoinICO.connect(alice).claimToken(alice.address))
                .to.be.revertedWith("NOT_READY_TO_CLAIM");
            });
        });
        describe("invest and claim during OPEN phase", function () {
            this.beforeEach(async function() {
                // move to GENERAL phase
                await spaceCoinICO.connect(owner).changeICOStage(1);
                await spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("100")});

                // move to OPEN phase
                await spaceCoinICO.connect(owner).changeICOStage(2);
            });
            it("claim token manually for all the investments made prior to OPEN phase", async function() {
                await spaceCoinICO.connect(alice).claimToken(alice.address);
                expect(await spaceCoinICO.connect(alice).balanceOf(alice.address)).to.equal(ethers.utils.parseEther("500"));
            });
            it("automatic release of token for all the investments made during OPEN phase", async function() {
                await spaceCoinICO.connect(alice).investInSPC({value: ethers.utils.parseEther("100")});
                expect(await spaceCoinICO.connect(alice).balanceOf(alice.address)).to.equal(ethers.utils.parseEther("1000"));

                await spaceCoinICO.connect(addrs[0]).investInSPC({value: ethers.utils.parseEther("100")});
                expect(await spaceCoinICO.connect(owner).balanceOf(addrs[0].address)).to.equal(ethers.utils.parseEther("500"));
            });
        });
    });
});