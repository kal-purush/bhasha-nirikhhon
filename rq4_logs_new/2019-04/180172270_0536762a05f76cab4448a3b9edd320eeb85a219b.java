package HW14.abstractFactory.factories;

import HW14.abstractFactory.individs.canFly.FlyAble;
import HW14.abstractFactory.individs.canFly.MarsFlyer;
import HW14.abstractFactory.individs.canRun.MarsRunner;
import HW14.abstractFactory.individs.canRun.RunAble;
import HW14.abstractFactory.individs.canWork.MarsWorker;
import HW14.abstractFactory.individs.canWork.WorkAble;

public class MarsianFactory implements AbstractFactory{
    @Override
    public FlyAble getFlyer() {
        return new MarsFlyer();
    }

    @Override
    public RunAble getRunner() {
        return new MarsRunner();
    }

    @Override
    public WorkAble getWorker() {
        return new MarsWorker();
    }
}