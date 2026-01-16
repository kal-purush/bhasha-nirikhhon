{\rtf1\ansi\ansicpg1252\cocoartf2639
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier-Oblique;\f1\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;\red121\green121\blue121;\red0\green29\blue164;\red9\green9\blue9;
\red10\green79\blue103;\red18\green51\blue230;\red17\green109\blue18;\red114\green0\blue130;}
{\*\expandedcolortbl;;\csgenericrgb\c47451\c47451\c47451;\csgenericrgb\c0\c11373\c64314;\csgenericrgb\c3529\c3529\c3529;
\csgenericrgb\c3922\c30980\c40392;\csgenericrgb\c7059\c20000\c90196;\csgenericrgb\c6667\c42745\c7059;\csgenericrgb\c44706\c0\c50980;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx560\tx1120\tx1680\tx2240\tx2800\tx3360\tx3920\tx4480\tx5040\tx5600\tx6160\tx6720\pardirnatural\partightenfactor0

\f0\i\fs26 \cf2 //https://www.geeksforgeeks.org/check-for-balanced-parentheses-in-an-expression/\
//Time Complexity: O(N), Iteration over the string of size N one time.\
//Auxiliary Space: O(N) for stack.\
\
/*\
Follow the steps mentioned below to implement the idea:\
\
Declare a character stack (say temp).\
Now traverse the string exp.\
If the current character is a starting bracket ( \'91(\'91 or \'91\{\'91  or \'91[\'91 ) then push it to stack.\
If the current character is a closing bracket ( \'91)\'92 or \'91\}\'92 or \'91]\'92 ) then pop from stack and if the popped character is the matching starting bracket then fine.\
Else brackets are Not Balanced.\
After complete traversal, if there is some starting bracket left in stack then Not balanced, else Balanced.\
 */\
\

\f1\i0 \cf3 import \cf0 java.util.ArrayDeque\cf4 ;\
\cf3 import \cf0 java.util.Deque\cf4 ;\
\
\cf3 public class \cf0 BalanceCheckParamUsingStack \cf4 \{\
\
    
\f0\i \cf2 // function to check if brackets are balanced\
    
\f1\i0 \cf3 static boolean \cf5 areBracketsBalanced\cf4 (\cf0 String \cf4 expr) \{\
        
\f0\i \cf2 // Using ArrayDeque is faster than using Stack class\
        
\f1\i0 \cf0 Deque\cf4 <\cf0 Character\cf4 > \cf0 stack\
                \cf4 = \cf3 new \cf4 ArrayDeque<\cf0 Character\cf4 >();\
\
        
\f0\i \cf2 // Traversing the Expression\
        
\f1\i0 \cf3 for \cf4 (\cf3 int \cf0 i \cf4 = \cf6 0\cf4 ; \cf0 i \cf4 < expr.length(); \cf0 i\cf4 ++) \{\
            \cf3 char \cf0 x \cf4 = expr.charAt(\cf0 i\cf4 );\
\
            \cf3 if \cf4 (\cf0 x \cf4 == \cf7 '(' \cf4 || \cf0 x \cf4 == \cf7 '[' \cf4 || \cf0 x \cf4 == \cf7 '\{'\cf4 ) \{\
                
\f0\i \cf2 // Push the element in the stack\
                
\f1\i0 \cf0 stack\cf4 .push(\cf0 x\cf4 );\
                \cf3 continue\cf4 ;\
            \}\
\
            
\f0\i \cf2 // If current character is not opening\
            // bracket, then it must be closing. So stack\
            // cannot be empty at this point.\
            
\f1\i0 \cf3 if \cf4 (\cf0 stack\cf4 .isEmpty())\
                \cf3 return false\cf4 ;\
            \cf3 char \cf0 check\cf4 ;\
            \cf3 switch \cf4 (\cf0 x\cf4 ) \{\
                \cf3 case \cf7 ')'\cf4 :\
                    \cf0 check \cf4 = \cf0 stack\cf4 .pop();\
                    \cf3 if \cf4 (\cf0 check \cf4 == \cf7 '\{' \cf4 || \cf0 check \cf4 == \cf7 '['\cf4 )\
                        \cf3 return false\cf4 ;\
                    \cf3 break\cf4 ;\
\
                \cf3 case \cf7 '\}'\cf4 :\
                    \cf0 check \cf4 = \cf0 stack\cf4 .pop();\
                    \cf3 if \cf4 (\cf0 check \cf4 == \cf7 '(' \cf4 || \cf0 check \cf4 == \cf7 '['\cf4 )\
                        \cf3 return false\cf4 ;\
                    \cf3 break\cf4 ;\
\
                \cf3 case \cf7 ']'\cf4 :\
                    \cf0 check \cf4 = \cf0 stack\cf4 .pop();\
                    \cf3 if \cf4 (\cf0 check \cf4 == \cf7 '(' \cf4 || \cf0 check \cf4 == \cf7 '\{'\cf4 )\
                        \cf3 return false\cf4 ;\
                    \cf3 break\cf4 ;\
            \}\
        \}\
\
        
\f0\i \cf2 // Check Empty Stack\
        
\f1\i0 \cf3 return \cf4 (\cf0 stack\cf4 .isEmpty());\
    \}\
\
    \cf3 public static void \cf5 main\cf4 (\cf0 String\cf4 [] args) \{\
        \cf0 String expr \cf4 = \cf7 "([\{\}])"\cf4 ;\
\
        
\f0\i \cf2 // Function call\
        
\f1\i0 \cf3 if \cf4 (\cf0 BalanceCheckParamUsingStack\cf4 .
\f0\i areBracketsBalanced
\f1\i0 (\cf0 expr\cf4 ))\
            \cf0 System\cf4 .
\f0\i \cf8 out
\f1\i0 \cf4 .println(\cf7 "Balanced "\cf4 );\
        \cf3 else\
            \cf0 System\cf4 .
\f0\i \cf8 out
\f1\i0 \cf4 .println(\cf7 "Not Balanced "\cf4 );\
    \}\
\}\
\
}