import { Robot, Facing } from "./Robot";

describe("Robot", () => {
  describe("commands", () => {
    describe("place", () => {
      it("creates a placed robot", () => {
        const robot = Robot.place(1, 1, Facing.South);

        expect(robot.x).toBe(1);
        expect(robot.y).toBe(1);
        expect(robot.f).toBe(Facing.South);
      });

      it("cannot create a robot placed beyond minimum x position", () => {
        expect(() => {
          Robot.place(-1, 0, Facing.North);
        }).toThrow("invalid x");
      });

      it("cannot create a robot placed beyond maximum x position", () => {
        expect(() => {
          Robot.place(10, 0, Facing.North);
        }).toThrow("invalid x");
      });

      it("cannot create a robot placed beyond minimum y position", () => {
        expect(() => {
          Robot.place(0, -1, Facing.North);
        }).toThrow("invalid y");
      });

      it("cannot create a robot placed beyond maximum y position", () => {
        expect(() => {
          Robot.place(0, 10, Facing.North);
        }).toThrow("invalid y");
      });

      // @TODO: talk about this
      // it("cannot create a robot with invalid facing", () => {
      //     expect(() => {
      //         Robot.place(0, 10, 4)
      //     }).toThrow("invalid facing")
      // })

      it("cannot create a robot with invalid facing", () => {
        expect(() => {
          Robot.place(0, 10, 5);
        }).toThrow("invalid facing");
        expect(() => {
          Robot.place(0, 3, -9);
        }).toThrow("invalid facing");
      });
    });
  });

  describe("left", () => {
    it("rotates the robot counter-clockwise", () => {
      const robot = Robot.place(0, 0, Facing.East);

      robot.left();

      expect(robot.f).toBe(Facing.North);

      robot.left();

      expect(robot.f).toBe(Facing.West);

      robot.left();

      expect(robot.f).toBe(Facing.South);

      robot.left();

      expect(robot.f).toBe(Facing.East);
    });
  });

  describe("right", () => {
    it("rotates the robot clockwise", () => {
      const robot = Robot.place(0, 0, Facing.South);

      robot.right();

      expect(robot.f).toBe(Facing.West);

      robot.right();

      expect(robot.f).toBe(Facing.North);

      robot.right();

      expect(robot.f).toBe(Facing.East);

      robot.right();

      expect(robot.f).toBe(Facing.South);
    });
  });

  describe("move", () => {
    it("when the robot is facing west it moves west", () => {
      const robot = Robot.place(3, 3, Facing.West);

      robot.move();

      expect(robot.x).toBe(2);
      expect(robot.y).toBe(3);
    });

    it("when the robot is facing south it moves south", () => {
      const robot = Robot.place(3, 3, Facing.South);

      robot.move();

      expect(robot.x).toBe(3);
      expect(robot.y).toBe(2);
    });

    it("when the robot is facing north it moves north", () => {
      const robot = Robot.place(3, 3, Facing.North);

      robot.move();

      expect(robot.x).toBe(3);
      expect(robot.y).toBe(4);
    });

    it("when the robot is facing east it moves east", () => {
      const robot = Robot.place(3, 3, Facing.East);

      robot.move();

      expect(robot.x).toBe(4);
      expect(robot.y).toBe(3);
    });

    it("does not fall off the sides of the table", () => {
      const robot = Robot.place(0, 0, Facing.South);

      robot.move();

      expect(robot.x).toBe(0);
      expect(robot.y).toBe(0);
    });
  });

  describe("report", () => {
    it("returns a human readable report of the robot's state", () => {
      const robot = Robot.place(0, 0, Facing.South);
      expect(robot.report()).toEqual("0, 0, South");
    });
  });
});