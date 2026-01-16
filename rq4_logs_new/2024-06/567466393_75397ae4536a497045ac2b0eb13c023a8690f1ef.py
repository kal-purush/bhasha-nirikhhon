from runSimulation import run_simulation, run_input, test_full_banana, print_gene


def run_genetic(plan, unit):
    run_simulation(plan, unit)
    if test_full_banana():
        print_gene([100] * 10000)
        return
    else:
        run_input([100]*1000)