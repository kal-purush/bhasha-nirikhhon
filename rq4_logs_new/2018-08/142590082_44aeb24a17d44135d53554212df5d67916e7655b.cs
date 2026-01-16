using Xunit;
using LeetCodeSolutions;

namespace LeetCodeTests
{
    public class Problem391Test
    {
        [Fact]
        [Trait("Category", "Basic Passing")]
        public void BasicPass1(){
            //Arrange
            int[,] rectangles = new int[,] { 
                {1,1,3,3},
                {3,1,4,2},
                {3,2,4,4},
                {1,3,2,4},
                {2,3,3,4}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category", "Basic Passing")]
        public void BasicPass2()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,2},
                {2,1,3,2},
                {1,2,2,3},
                {2,2,3,3},
                {3,1,4,4},
                {1,3,3,5},
                {3,4,4,5}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(Problem391.Solve(rectangles));
        }

        [Fact]
        [Trait("Category", "Basic Passing")]
        public void BasicPass3()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {3,3,15,22}
            };

            //Act
            bool result = Problem391.Solve(rectangles);
            
            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category", "Basic Passing")]
        public void BasicPass4()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,1,5},
                {2,1,2,5},
                {3,1,3,5},
                {4,1,4,5},
                {5,1,5,5},
                {6,1,6,5}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category", "Basic Failing")]
        public void BasicFail1()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,3,3},
                {3,1,4,2},
                {3,2,4,4},
                {2,3,3,4}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Basic Failing")]
        public void BasicFail2()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {2,1,3,2},
                {1,2,2,3},
                {2,2,3,3},
                {1,1,2,2},
                {3,1,4,4},
                {3,4,4,5}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Basic Failing")]
        public void BasicFail3()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {3,3,15,22},
                {14,22,15,23}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Basic Failing")]
        public void BasicFail4()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,1,5},
                {2,1,2,5},
                {3,1,3,5},
                {4,1,4,5},
                {5,1,5,5},
                {6,1,6,4}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Overlaps")]
        public void Overlap1()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {5,5,6,10},
                {5,9,6,15}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Overlaps")]
        public void Overlap2()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,3,3},
                {3,1,4,2},
                {1,3,2,4},
                {2,2,4,4}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Overlaps")]
        public void Overlap3()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,2},
                {1,1,2,2}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Negative Points")]
        public void NegativePointsPass()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {-2,-2,-1,2},
                {-1,-2, 3,2}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category","Negative Points")]
        public void NegativePointsFail()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {-1,-1,-1,-5},
                {-2,-1,-2,-5},
                {-3,-1,-3,-5},
                {-4,-1,-4,-5},
                {-5,-1,-5,-5},
                {-6,-1,-6,-4}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Unusual Topology")]
        public void NotConnected1()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,5},
                {3,1,4,5}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Unusual Topology")]
        public void NotConnected2()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,2},
                {3,1,4,2},
                {1,3,2,4},
                {3,3,4,4}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Unusual Topology")]
        public void NotSimplyConnected1()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,2},
                {2,1,3,2},
                {1,2,2,3},
                {3,1,4,4},
                {1,3,3,5},
                {3,4,4,5}
            };

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Unusual Topology")]
        public void NotSimplyConnected2()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,2,10},
                {1,10,10,10},
                {2,1,11,2},
                {10,2,11,11}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Large Area")]
        public void LargeCasePass()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,1000000,1000000 },
                {1,1000000, 1000000,1000001},
                {1000000, 1, 1000001,1000001}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category", "Large Area")]
        public void LargeCaseFail()
        {
            //Arrange
            int[,] rectangles = new int[,] {
                {1,1,1000000,1000000 },
                {1,1000000, 1000000,1000001},
                {1000000, 1, 1000001,1000002}
            };
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

        [Fact]
        [Trait("Category", "Many Rectangles")]
        public void ManyRectanglesPass()
        {
            //Arrange
            int[,] rectangles = new int[1000000,4];
            int rectNumber = 0;
            for (int i = 0; i < 1000; i++)
            {
                for (int j = 0; j < 1000; j++)
                {
                    rectangles[rectNumber,0] = i;
                    rectangles[rectNumber,1] = j;
                    rectangles[rectNumber,2] = i+1;
                    rectangles[rectNumber,3] = j+1;
                    rectNumber++;
                }
            }
            
            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.True(result);
        }

        [Fact]
        [Trait("Category", "Many Rectangles")]
        public void ManyRectanglesfail()
        {
            //Arrange
            int[,] rectangles = new int[1000001, 4];
            int rectNumber = 0;
            for (int i = 0; i < 1000; i++)
            {
                for (int j = 0; j < 1000; j++)
                {
                    rectangles[rectNumber, 0] = i;
                    rectangles[rectNumber, 1] = j;
                    rectangles[rectNumber, 2] = i + 1;
                    rectangles[rectNumber, 3] = j + 1;
                    rectNumber++;
                }
            }
            rectangles[1000000, 0] = 1000000;
            rectangles[1000000, 1] = 1000000;
            rectangles[1000000, 2] = 1000001;
            rectangles[1000000, 3] = 1000001;

            //Act
            bool result = Problem391.Solve(rectangles);

            //Assert
            Assert.False(result);
        }

    }
}