using AdventOfCodeUtilities.Helpers;

namespace AdventOfCode.Days;

internal class Day08 : BaseDay
{
    private readonly int[][] _input;

    public Day08()
    {
        _input = File.ReadAllLines(InputFilePath).Select(x => x.ToCharArray().Select(x => x - '0').ToArray()).ToArray();
    }

    public override ValueTask<string> Solve_1()
    {
        var visibleTrees = 0;

        for (var i = 0; i < _input.Length; i++)
        {
            for (var j = 0; j < _input[0].Length; j++)
            {
                var treeVisible = i == 0 || j == 0 || i == _input.Length - 1 || j == _input[0].Length - 1;

                // Up
                var up = i;
                while (!treeVisible && ArrayHelper.IsValidCoordinate(up, j, _input))
                {
                    up--;
                    if (_input[up][j] < _input[i][j])
                    {
                        if (up == 0)
                        {
                            treeVisible = true;
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                // Left
                var left = j;

                while (!treeVisible && ArrayHelper.IsValidCoordinate(i, left, _input))
                {
                    left--;
                    if (_input[i][left] < _input[i][j])
                    {
                        if (left == 0)
                        {
                            treeVisible = true;
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }

                }

                // Right
                var right = j;

                while (!treeVisible && ArrayHelper.IsValidCoordinate(i, right + 1, _input))
                {
                    right++;
                    if (_input[i][right] < _input[i][j])
                    {
                        if (right == _input[0].Length - 1)
                        {
                            treeVisible = true;
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }

                }

                // Down
                var down = i;

                while (!treeVisible && ArrayHelper.IsValidCoordinate(down + 1, j, _input))
                {
                    down++;
                    if (_input[down][j] < _input[i][j])
                    {
                        if (down == _input.Length - 1)
                        {
                            treeVisible = true;
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                if (treeVisible)
                {
                    visibleTrees++;
                }
            }
        }

        return new(visibleTrees.ToString());
    }

    public override ValueTask<string> Solve_2()
    {
        var highestScenicScore = 0;
        for (var i = 0; i < _input.Length; i++)
        {
            for (var j = 0; j < _input[0].Length; j++)
            {
                var scoreArray = new int[4];

                // Up
                var up = i;

                while (ArrayHelper.IsValidCoordinate(up - 1, j, _input))
                {
                    up--;

                    if (_input[up][j] < _input[i][j])
                    {
                        if (up == 0)
                        {
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                scoreArray[0] = Math.Abs(i - up);

                // Left
                var left = j;

                while (ArrayHelper.IsValidCoordinate(i, left - 1, _input))
                {
                    left--;
                    if (_input[i][left] < _input[i][j])
                    {
                        if (left == 0)
                        {
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }

                }

                scoreArray[1] = Math.Abs(j - left);

                // Right
                var right = j;

                while (ArrayHelper.IsValidCoordinate(i, right + 1, _input))
                {
                    right++;
                    if (_input[i][right] < _input[i][j])
                    {
                        if (right == _input[0].Length - 1)
                        {
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                scoreArray[2] = Math.Abs(j - right);


                // Down
                var down = i;

                while (ArrayHelper.IsValidCoordinate(down + 1, j, _input))
                {
                    down++;
                    if (_input[down][j] < _input[i][j])
                    {
                        if (down == _input.Length - 1)
                        {
                            break;
                        }
                    }
                    else
                    {
                        break;
                    }
                }

                scoreArray[3] = Math.Abs(i - down);

                var scenicScore = scoreArray[0] * scoreArray[1] * scoreArray[2] * scoreArray[3];
                if (scenicScore > highestScenicScore)
                    highestScenicScore = scenicScore;
            }
        }

        return new(highestScenicScore.ToString());
    }
}