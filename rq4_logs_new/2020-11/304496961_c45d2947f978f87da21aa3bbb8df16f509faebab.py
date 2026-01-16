from breezypythongui import EasyFrame

class BouncyGUI(EasyFrame):
    """Window for program"""
           
    def __init__(self):
        """Setting up the window"""
        EasyFrame.__init__(self, title = "Bouncy")

        #Height label and field
        self.addLabel(text = "Initial height", row = 0, column = 0)
        self.heightField = self.addFloatField(value = 0.0, row = 0, column = 1)

        #Index label and field
        self.addLabel(text = "Index", row = 1, column = 0)
        self.indexField = self.addFloatField(value = 0.0, row = 1, column = 1)

        #Number of bounces label and field
        self.addLabel(text = "Number of bounces", row = 2, column = 0)
        self.bounceField = self.addIntegerField(value = 0, row = 2, column = 1)

        #Compute button
        self.addButton(text = "Compute", row = 3, column = 0,
                           columnspan = 2, command = self.computeDistance)

        #Distance label and field
        self.addLabel(text = "Total distance", row = 4, column = 0)
        self.distanceField = self.addFloatField(value = 0.0, row = 4, column = 1,
                                              precision = 2)

    #Computations for the button
    def computeDistance(self):
        height = self.heightField.getNumber()
        index = self.indexField.getNumber()
        bounces = self.bounceField.getNumber()
        bounceHeight = height * index
        distance = 0
        for count in range(bounces):
            distance += (height + bounceHeight)
            height = bounceHeight
            bounceHeight = bounceHeight * index
            self.distanceField.setNumber(distance)

def main():
    BouncyGUI().mainloop()

if __name__ == "__main__":
    main()

"""
Original Code:
initialHeight = float(input("Enter the initial height of the drop in feet: "))
bounce = int(input("Enter how many times the ball is allowed to bounce: "))
bounceHeight = initialHeight * 0.6
height = initialHeight
distance = 0


for count in range(bounce):
    distance = distance + height + bounceHeight
    height = bounceHeight
    bounceHeight = bounceHeight * 0.6


print("The total distance traveled is: %0.2f" % distance + " ft")
"""