using System;
using System.Collections.Generic;
using SummerTask_RadkevichMarsell.blockScheme.blocks;
using SummerTask_RadkevichMarsell.blockScheme.Links;
using SummerTask_RadkevichMarsell.tokenization.tokens;

namespace SummerTask_RadkevichMarsell.blockScheme
{
    public class BlockBuilder
    {

        public BlockTemplate[] Build(List<TokenRecord> tokens)
        {
            var dict = new Dictionary<Expression, BlockTemplate>();
            var blocks = CreateBlocks(tokens, dict);
            SetDefaultLinks(blocks);
            var stack = new Stack<TokenRecord>();
            var mainToken = new StartEndToken("Main");
            mainToken.InnerTokens = tokens;
            stack.Push(mainToken);
            SetLinks(dict, stack);

            return blocks;
        }

        private BlockTemplate[] CreateBlocks(List<TokenRecord> tokens, Dictionary<Expression, BlockTemplate> dict)
        {
            var blocks = new List<BlockTemplate>();
            BlockTemplate currentBlock;

            foreach (var token in tokens)
            {
                switch (token)
                {
                    case StartEndToken startEndToken:
                        currentBlock = new BlockTemplate(BlockType.StartEnd, startEndToken.Value.Text);
                        dict[startEndToken.Value] = currentBlock;
                        blocks.Add(currentBlock);
                        break;

                    case OperationToken operationToken:
                        currentBlock = new BlockTemplate(BlockType.Operation, operationToken.Value.Text);
                        dict[operationToken.Value] = currentBlock;
                        blocks.Add(currentBlock);
                        break;

                    case IfToken ifToken:
                        currentBlock = new BlockTemplate(BlockType.Question, ifToken.Condition.Text);
                        dict[ifToken.Condition] = currentBlock;
                        blocks.Add(currentBlock);
                        blocks.AddRange(CreateBlocks(ifToken.InnerTokens, dict));
                        break;

                    case ElseToken elseToken:
                        blocks.AddRange(CreateBlocks(elseToken.InnerTokens, dict));
                        break;

                    case WhileToken whileToken:
                        currentBlock = new BlockTemplate(BlockType.Question, whileToken.Condition.Text);
                        dict[whileToken.Condition] = currentBlock;
                        blocks.Add(currentBlock);
                        blocks.AddRange(CreateBlocks(whileToken.InnerTokens, dict));
                        break;

                    case ForToken forToken:
                        currentBlock = new BlockTemplate(BlockType.Operation, forToken.Initialization.Text);
                        dict[forToken.Initialization] = currentBlock;
                        blocks.Add(currentBlock);

                        currentBlock = new BlockTemplate(BlockType.Question, forToken.Condition.Text);
                        dict[forToken.Condition] = currentBlock;
                        blocks.Add(currentBlock);

                        blocks.AddRange(CreateBlocks(forToken.InnerTokens, dict));

                        currentBlock = new BlockTemplate(BlockType.Operation, forToken.Iteration.Text);
                        dict[forToken.Iteration] = currentBlock;
                        blocks.Add(currentBlock);
                        break;

                    default:
                        throw new Exception();
                }
            }
            return blocks.ToArray();
        }

        private void SetLinks(Dictionary<Expression, BlockTemplate> dict, Stack<TokenRecord> stack)
        {
            var tokens = stack.Peek().InnerTokens;
            for (var i = 0; i < tokens.Count; i++)
            {
                switch (tokens[i])
                {
                    case StartEndToken _:
                    case OperationToken _:
                        continue;
                    case IfToken ifToken:
                        SetLinksForIf(dict, i, stack);
                        stack.Push(ifToken);
                        SetLinks(dict, stack);
                        if (stack.Peek() == ifToken)
                            stack.Pop();
                        break;
                    case ElseToken elseToken:
                        SetLinksForElse(dict, i, stack);
                        stack.Push(elseToken);
                        SetLinks(dict, stack);
                        if (stack.Peek() == elseToken)
                            stack.Pop();
                        break;
                    case WhileToken whileToken:
                        SetLinksForWhile(dict, i, stack);
                        stack.Push(whileToken);
                        SetLinks(dict, stack);
                        if (stack.Peek() == whileToken)
                            stack.Pop();
                        break;
                    case ForToken forToken:
                        SetLinksForFor(dict, i, stack);
                        stack.Push(forToken);
                        SetLinks(dict, stack);
                        if (stack.Peek() == forToken)
                            stack.Pop();
                        break;
                    default:
                        throw new Exception();
                }
            }
        }

        private void SetDefaultLinks(BlockTemplate[] blocks)
        {
            for (int i = 0; i < blocks.Length - 1; i++)
            {
                blocks[i].SetLinks(new BaseLink[]
                {
                    new Link(blocks[i], blocks[i+1])
                });
            }
        }

        private void SetLinksForIf(
            Dictionary<Expression, BlockTemplate> dict,
            int i,
            Stack<TokenRecord> stack)
        {
            var ifToken = (IfToken)stack.Peek().InnerTokens[i];
            var condtionBlock = dict[ifToken.Condition];
            var firstOperBlock = dict[ifToken.InnerTokens[0].GetFirstTokenExpression()];
            var trueLink = new TrueLink(condtionBlock, firstOperBlock);
            while (i + 1 >= stack.Peek().InnerTokens.Count)
            {
                var token = stack.Pop();
                i = stack.Peek().InnerTokens.IndexOf(token);
            }
            var nextToken = stack.Peek().InnerTokens[i + 1];
            firstOperBlock = dict[nextToken.GetFirstTokenExpression()];
            var falseLink = new FalseLink(condtionBlock, firstOperBlock);
            condtionBlock.SetLinks(new BaseLink[] { trueLink, falseLink });

            var lastOperBlock = dict[ifToken.GetLastTokenExpression()];
            while(nextToken is ElseToken)
            {
                if (i + 2 < stack.Peek().InnerTokens.Count) 
                {
                    nextToken = stack.Peek().InnerTokens[i + 2];
                    break;
                }
                else while (i + 1 >= stack.Peek().InnerTokens.Count)
                {
                    var token = stack.Pop();
                    i = stack.Peek().InnerTokens.IndexOf(token);
                }
                nextToken = stack.Peek().InnerTokens[i + 1];
            }

            firstOperBlock = dict[nextToken.GetFirstTokenExpression()];
            lastOperBlock.SetLinks(new BaseLink[] { new Link(lastOperBlock, firstOperBlock) });
        }

        private void SetLinksForWhile(
            Dictionary<Expression, BlockTemplate> dict, 
            int i,
            Stack<TokenRecord> stack)
        {
            var whileToken = (WhileToken)stack.Peek().InnerTokens[i];
            var condtionBlock = dict[whileToken.Condition];
            var firstOperBlock = dict[whileToken.InnerTokens[0].GetFirstTokenExpression()];
            var trueLink = new TrueLink(condtionBlock, firstOperBlock);
            while (i + 1 >= stack.Peek().InnerTokens.Count)
            {
                var token = stack.Pop();
                i = stack.Peek().InnerTokens.IndexOf(token);
            }
            var nextToken = stack.Peek().InnerTokens[i + 1];
            firstOperBlock = dict[nextToken.GetFirstTokenExpression()];
            var falseLink = new FalseLink(condtionBlock, firstOperBlock);
            condtionBlock.SetLinks(new BaseLink[] { trueLink, falseLink });

            var lastOperBlock = dict[whileToken.GetLastTokenExpression()];
            lastOperBlock.SetLinks(new BaseLink[] { new Link(lastOperBlock, condtionBlock) });
        }

        private void SetLinksForElse(
            Dictionary<Expression, BlockTemplate> dict, 
            int i,
            Stack<TokenRecord> stack)
        {
            var elseToken = (ElseToken)stack.Peek().InnerTokens[i];
            var lastOperBlock = dict[elseToken.GetLastTokenExpression()];
            while (i + 1 >= stack.Peek().InnerTokens.Count)
            {
                var token = stack.Pop();
                i = stack.Peek().InnerTokens.IndexOf(token);
            }
            var nextToken = stack.Peek().InnerTokens[i + 1];
            var firstOperBlock = dict[nextToken.GetFirstTokenExpression()];
            lastOperBlock.SetLinks(new BaseLink[] { new Link(lastOperBlock, firstOperBlock) });
        }

        private void SetLinksForFor(
            Dictionary<Expression, BlockTemplate> dict, 
            int i, 
            Stack<TokenRecord> stack)
        {
            var forToken = (ForToken)stack.Peek().InnerTokens[i];
            var condtionBlock = dict[forToken.Condition];
            var firstOperBlock = dict[forToken.InnerTokens[0].GetFirstTokenExpression()];
            var trueLink = new TrueLink(condtionBlock, firstOperBlock);
            while (i + 1 >= stack.Peek().InnerTokens.Count)
            {
                var token = stack.Pop();
                i = stack.Peek().InnerTokens.IndexOf(token);
            }
            var nextToken = stack.Peek().InnerTokens[i + 1];
            firstOperBlock = dict[nextToken.GetFirstTokenExpression()];
            var falseLink = new FalseLink(condtionBlock, firstOperBlock);
            condtionBlock.SetLinks(new BaseLink[] { trueLink, falseLink });

            var lastOperBlock = dict[forToken.GetLastTokenExpression()];
            lastOperBlock.SetLinks(new BaseLink[] { new Link(lastOperBlock, condtionBlock) });
        }
    }
}