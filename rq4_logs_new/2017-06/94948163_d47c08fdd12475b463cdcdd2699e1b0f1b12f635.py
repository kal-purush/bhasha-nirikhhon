from tieronepointfive.enums import State, Transition

class MockEvaluator:
    def evaluate_tick(self, tick):
        tick.complete(Transition.ALL_SITES_REACHED, State.CONNECTION_WORKING)
        return tick