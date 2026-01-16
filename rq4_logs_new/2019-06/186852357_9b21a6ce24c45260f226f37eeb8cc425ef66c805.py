import matplotlib.pyplot as plt
import numpy as np

class Plotter():

    def load_file(self, text: str):
        fh = open(text, 'r')
        positions = [line.split() for line in fh]

        temperatures = []

        for x in positions:
            for y in range(len(positions[0])):
                if y == 4:
                    temperatures.append(float(x[y]))

        x = np.arange(0, len(temperatures) * 0.00266666667, 0.00266666667)
        y = np.array(temperatures)
        print(y)
        print(x)
        plt.plot(x, y)
        plt.ylabel('Temperature [°C]')
        plt.xlabel('Time [min]')
        plt.show()

if __name__ == "__main__":

    plot = Plotter()
    plt.close('all')
    plot.load_file('Test 1 _4deg_defaultPID.txt')
    plot.load_file('Test 2 _4deg_autotunedPID.txt')
    plot.load_file('Test 3 _4deg _10 deg_15deg_6deg_1deg.txt')
    plot.load_file('Test 5_4deg_40deg syringe_fire screwdriver.txt')
    plot.load_file('Test_Syringe_4deg_5deg_10deg_15deg.txt')

