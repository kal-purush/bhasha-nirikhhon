from simulator import Simulation
from utils import display_setting, modify_rou_flow_rate
from stats import Statistics
import setting

def main():

    # Display the simulation settings
    display_setting()

    # Create an instance of the Simulation class (it will handle the simulation logic)
    # And the Statistics class  (at the end of the simulation it will handle statistics with the recorded data)
    sim = Simulation(setting.STEP, setting.WARM_UP, setting.RUNS)
    stats = Statistics(setting.NAME)    

    # For each flow rate in the list, modify the XML file and run the simulation
    for i in setting.TANGENTIAL_FLOW:
        modify_rou_flow_rate(setting.ROU_PATH, i, setting.STEP)
    
        """ Run the simulation with all possible combinations of parameters """
        config = []
        for offset in range(0, setting.CONFIGURATION, setting.CONFIGURATION_STEP):
            print(f"====== Running configuration {offset//setting.CONFIGURATION_STEP} ======")
            config.append(sim.specific_config(offset))

        name = f"results/{setting.NAME}/{str(i).replace('.', '')}/"
        stats.name = name    

        stats.evaluate_metrics(config)


if __name__ == "__main__":
    main()