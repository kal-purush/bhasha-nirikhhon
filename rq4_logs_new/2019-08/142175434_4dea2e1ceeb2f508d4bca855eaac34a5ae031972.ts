import { expect } from 'chai';
import { calculateSpeed } from "../src/utils";
import config from "../src/constants/config"
import "mocha";

describe("Utils", () => {
  describe ("calculateSpeed", () => {
    it("should calculate the speed given rotation angle 0", () => {
      let expectedSpeed = {x: config.pacMan.defaultSpeed, y: 0.0};
      expect(calculateSpeed(0)).to.eql(expectedSpeed);
    });
  
    it("should calculate the speed given rotation angle 90", () => {
      let expectedSpeed = {x: 0.0, y: config.pacMan.defaultSpeed};
      expect(calculateSpeed(90)).to.eql(expectedSpeed);
    });

    it("should calculate the speed given rotation angle 180", () => {
      let expectedSpeed = {x: -config.pacMan.defaultSpeed, y: 0.0};
      expect(calculateSpeed(180)).to.eql(expectedSpeed);
    });
  
    it("should calculate the speed given rotation angle 270", () => {
      let expectedSpeed = {x: 0.0, y: -config.pacMan.defaultSpeed};
      expect(calculateSpeed(270)).to.eql(expectedSpeed);
    });

  });
});