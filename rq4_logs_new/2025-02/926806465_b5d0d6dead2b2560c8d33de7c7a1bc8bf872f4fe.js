const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("DefenseContractor", function () {
  let defenseContractor;
  let addr1;

  const item = {
    GUNPOWDER: 0,
    METAL: 1,
    FUEL: 2,
    BULLET: 3,
    TANK: 4,
    FLARE: 5,
    MISSILE: 6,
  };

  beforeEach(async function () {
    [addr1] = await ethers.getSigners();

    const DefenseContractor = await ethers.getContractFactory(
      "DefenseContractor"
    );
    defenseContractor = await DefenseContractor.deploy();
  });

  describe("Order Item", () => {
    it("should allow ordering valid items", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      const balance = await defenseContractor.getBalanceOf(
        addr1,
        item.GUNPOWDER
      );
      expect(balance).to.equal(1, "The balance of GUNPOWDER should be 1");
    });

    it("should not allow ordering invalid items", async () => {
      const INVALID_ID = Object.keys(item).length + 1;
      await expect(
        defenseContractor.connect(addr1).orderItem(INVALID_ID)
      ).to.be.revertedWith("invalidID");
    });

    it("should enforce cooldown period", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      await expect(
        defenseContractor.connect(addr1).orderItem(item.GUNPOWDER)
      ).to.be.revertedWith("must wait 60 seconds");
    });

    it("should produce BULLET if user has GUNPOWDER and METAL", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);
      await defenseContractor.connect(addr1).orderItem(item.METAL);
      await defenseContractor.connect(addr1).orderItem(item.BULLET);
      const balance = await defenseContractor.getBalanceOf(addr1, item.BULLET);
      expect(balance).to.equal(1, "The balance of BULLET should be 1");
    });

    it("should produce TANK if user has METAL and FUEL", async () => {
      await defenseContractor.connect(addr1).orderItem(item.METAL);
      await defenseContractor.connect(addr1).orderItem(item.FUEL);
      await defenseContractor.connect(addr1).orderItem(item.TANK);
      const balance = await defenseContractor.getBalanceOf(addr1, item.TANK);
      expect(balance).to.equal(1, "The balance of TANK should be 1");
    });

    it("should produce FLARE if user has GUNPOWDER and FUEL", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);
      await defenseContractor.connect(addr1).orderItem(item.FUEL);
      await defenseContractor.connect(addr1).orderItem(item.FLARE);
      const balance = await defenseContractor.getBalanceOf(addr1, item.FLARE);
      expect(balance).to.equal(1, "The balance of FLARE should be 1");
    });

    it("should produce MISSILE if user has GUNPOWDER, METAL and FUEL", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);
      await defenseContractor.connect(addr1).orderItem(item.METAL);
      await defenseContractor.connect(addr1).orderItem(item.FUEL);
      await defenseContractor.connect(addr1).orderItem(item.MISSILE);
      const balance = await defenseContractor.getBalanceOf(addr1, item.MISSILE);
      expect(balance).to.equal(1, "The balance of MISSILE should be 1");
    });
  });

  describe("Trade Items", () => {
    it("should allow trading items", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      await defenseContractor
        .connect(addr1)
        .tradeItems(item.GUNPOWDER, item.METAL);

      const balanceGunpowder = await defenseContractor.getBalanceOf(
        addr1,
        item.GUNPOWDER
      );
      const balanceMetal = await defenseContractor.getBalanceOf(
        addr1,
        item.METAL
      );

      expect(balanceGunpowder).to.equal(
        0,
        "The balance of GUNPOWDER should be 0"
      );
      expect(balanceMetal).to.equal(1, "The balance of METAL should be 1");
    });

    it("should not allow trading the same item", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      await expect(
        defenseContractor
          .connect(addr1)
          .tradeItems(item.GUNPOWDER, item.GUNPOWDER)
      ).to.be.revertedWith("you can't trade the same item");
    });

    it("should not allow trading for invalid items", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      await expect(
        defenseContractor.connect(addr1).tradeItems(item.GUNPOWDER, item.BULLET)
      ).to.be.revertedWith("you can't trade for this item");
    });
  });

  describe("Dismantle Item", () => {
    it("should allow dismantling special items", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);
      await defenseContractor.connect(addr1).orderItem(item.METAL);
      await defenseContractor.connect(addr1).orderItem(item.BULLET);
      await defenseContractor.connect(addr1).dismantleItem(item.BULLET);

      const balance = await defenseContractor.getBalanceOf(addr1, item.BULLET);
      expect(balance).to.equal(0, "The balance of BULLET should be 0");
    });

    it("should not allow dismantling non-special items", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);
      await expect(
        defenseContractor.connect(addr1).dismantleItem(item.GUNPOWDER)
      ).to.be.revertedWith("this is not a special item");
    });
  });

  describe("Get Balance and URI", () => {
    it("should return the correct balance", async () => {
      await defenseContractor.connect(addr1).orderItem(item.GUNPOWDER);

      const balance = await defenseContractor.getBalanceOf(
        addr1,
        item.GUNPOWDER
      );
      expect(balance).to.equal(1, "The balance of GUNPOWDER should be 1");
    });

    it("should return the correct URI", async () => {
      const baseURI = "ipfs://QmNfA1EoUfPkXEMQP7KguEioNkZjEJs6TiRJFN2w9UPYWu/";

      const uri = await defenseContractor.connect(addr1).getUri(item.GUNPOWDER);

      expect(uri).to.equal(`${baseURI}${item.GUNPOWDER}.json`);
    });
  });
});