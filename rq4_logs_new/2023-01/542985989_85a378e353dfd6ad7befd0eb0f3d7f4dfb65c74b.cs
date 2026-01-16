namespace BattleSpace.Lib;
using Hwdtech;

public class ConstructingATree : ICommand {
    private string way;
    public ConstructingATree(string way) {
        this.way = way;
    }

    public void Execute() {
        var strategy = IoC.Resolve<Dictionary<int, object>>("Game.ConstructingATree");

        try {
            using (StreamReader reader = File.OpenText(way)) {
                string? line;
                while ((line = reader.ReadLine()) != null) {
                    var record = line.Split().Select(int.Parse).ToList();
                    PutInTree(record, strategy);
                }
            }
        }

        catch (FileNotFoundException e) {
            throw new FileNotFoundException(e.ToString());
        }
        catch (Exception e) {
            throw new Exception(e.ToString());
        }
    }

    private void PutInTree(List<int> list1, IDictionary<int, object> root) {
        var Tree = root;
        foreach (var item in list1) {
            Tree.TryAdd(item, new Dictionary<int, object>());
            Tree = (Dictionary<int, object>)Tree[item];
        }
    }
}