using Mithrill.MonsterBook.Application.Common.Adapters;
using Mithrill.MonsterBook.Application.Common.Builders;
using Xunit;

namespace Mithrill.MonsterBook.Application.Tests
{
    public class NpcDesignerTests
    {
        private readonly NpcDesigner<IGeneratedCreature> _npcDesigner;
        private readonly INpcBuilder<IGeneratedCreature> _npcBuilder;

        public NpcDesignerTests()
        {


            //_npcBuilder = new CreatureBuilder<IGeneratedCreature>();
            //_npcDesigner = new NpcDesigner<IGeneratedCreature>();
        }

        [Fact]
        public void Test1()
        {

        }
    }
}