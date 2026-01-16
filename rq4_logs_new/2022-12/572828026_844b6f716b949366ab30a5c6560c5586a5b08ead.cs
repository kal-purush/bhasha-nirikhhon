using Microsoft.CodeAnalysis;

public class SyntaxHelper{
    public bool GetParentSyntax<T>(SyntaxNode node, out T result) where T : SyntaxNode
    {
        result = null;

        if (node == null)
            return false;

        var currentNode = node;

        while (currentNode != null)
        {
            currentNode = currentNode.Parent;

            if (currentNode is T)
            {
                result = currentNode as T;
                return true;
            }
        }

        return false;
    }
}