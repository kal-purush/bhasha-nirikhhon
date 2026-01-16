namespace BattleSpace.Lib.Test;

public class InjectCommandStrategy : IStrategy {
    public object ExecuteStrategy(params object[] args) {
        var obj = (IUObject)args[0];
        var cmd = (ICommand)args[1];

        return new InjectCommand(obj, cmd);
    }
}