describe("Component Tests", () => {
  describe("Is Collision With", () => {
    const component = new Component(50, 50, 50, 50);
    const obstacle = new Wall(50, 50);
    it("should return a type boolean", () => {
      expect(typeof component.isCollisionWith(obstacle)).toBe("boolean");
    });
    it("should return true if component and obstacle are at the same position", () => {
      const component = new Component(655, 55, 50, 50);
      const obstacle = new Wall(655, 55);
      expect(component.isCollisionWith(obstacle)).toEqual(true);
    });
  });
});