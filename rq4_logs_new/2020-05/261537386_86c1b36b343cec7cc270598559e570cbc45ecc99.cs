using System;

namespace simpledsl
{
    public interface NodeVisitor
    {
        void VisitNode(Node n);
        // void VisitNil(NilNode n);
        void VisitLiteral(LiteralNode n);
        /*
        void VisitSymbol(SymbolNode n);
        void VisitAddSub(AddSubNode n);
        void VisitMulDiv(MulDivNode n);
        void VisitPow(PowNode n);
        void VisitNegate(NegateNode n);
        void VisitFx(FxNode n);
        void VisitRelational(RelationalNode n);
        void VisitLogic(LogicNode n);
        void VisitLogicNot(LogicNotNode n);
        */
    }

    public class NodeVisitorImpl : NodeVisitor
    {
        public void VisitNode(Node n) { /* empty */ }
        public void VisitLiteral(LiteralNode n) { /* empty */ }
        
        // void VisitNil(NilNode n);

    }

}