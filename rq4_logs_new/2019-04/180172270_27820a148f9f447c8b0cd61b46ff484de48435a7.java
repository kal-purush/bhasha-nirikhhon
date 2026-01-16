package hw14.abstractFactory.factories;

import hw14.abstractFactory.individs.canFly.FlyAble;
import hw14.abstractFactory.individs.canFly.MarsFlyer;
import hw14.abstractFactory.individs.canRun.MarsRunner;
import hw14.abstractFactory.individs.canRun.RunAble;
import hw14.abstractFactory.individs.canWork.MarsWorker;
import hw14.abstractFactory.individs.canWork.WorkAble;

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