class Solution {
    public boolean isValid(String s) {
        char[] chars = s.toCharArray();
        MyStack stack = new MyStack(chars.length);

        for (int i = 0; i < chars.length; ++i) {
            if (chars[i] == '(' || chars[i] == '[' || chars[i] == '{') {
                stack.push(chars[i]);
            }
            else if (chars[i] == ')') {
                char temp = stack.pop();
                if (temp == '(') {
                    continue;
                }
                else {
                    return false;
                }
            }
            else if (chars[i] == ']') {
                char temp = stack.pop();
                if (temp == '[') {
                    continue;
                }
                else {
                    return false;
                }
            }
            else if (chars[i] == '}') {
                char temp = stack.pop();
                if (temp == '{') {
                    continue;
                }
                else {
                    return false;
                }
            }
        }

        System.out.println(stack.size());
        if (stack.size() > 0) {
            return false;
        }
        else {
            return true;
        }
    }

}

class MyStack {
    char[] chars;
    int top = 0;

    MyStack(int size) {
        if (size < 0) {
            return;
        }

        chars = new char[size];
    }

    void push(char c) {
        chars[top] = c;
        ++top;
    }

    char pop() {
        if (top < 1) {
            return 'X';
        }

        --top;
        return chars[top];
    }

    int size() {
        return top;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        System.out.println(s.isValid("()"));
    }
}