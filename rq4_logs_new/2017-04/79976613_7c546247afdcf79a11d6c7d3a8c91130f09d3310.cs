using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace PPGit.Lib
{
    class mapStack
    {
        const int STACK_SIZE = 5;
        Image[] stack;
        int x;
        private static mapStack instance = null;
        private mapStack() {
            stack = new Image[STACK_SIZE]; //initialize the stack
            x = 0; //number of elements in the stack
        }
        public static mapStack map {
            get {
                if (instance == null) {
                    instance = new mapStack();
                }
                return instance;
            }
        }
        public Image pushPop {
            get {
                if (x > 0)
                {
                    Image pop = stack[0];
                    for (int y = 0; y < x - 1; y++) { //Move all values down
                        stack[y] = stack[y + 1];
                    }
                    x--;
                    return pop;
                }
                else return null;
            }
            set {
                if (x > 0 && x != STACK_SIZE)
                {
                    for (int y = x; y > 0; y--)
                    { //Move all values up
                        stack[y] = stack[y - 1];
                    }
                    stack[0] = value; // Add new image value to stack
                    x++;
                }
                else if (x > 0 && x == STACK_SIZE)
                {
                    for (int y = x - 1; y > 0; y--) // Push everything up, erasing the last item
                    {
                        stack[y] = stack[y - 1];
                    }
                    stack[0] = value;
                }
                else {
                    stack[0] = value;
                    x++;
                }
            }
        }
    }
}