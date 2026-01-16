"""
Ananya Rajgarhia
1/20/17
NOTE: numericStringParser requires pyparsing (currently not using numericStringParser though)
Ask Sarah:
  - what do you think syntax for function should be
  - how git (like if I wanna pull/merge the UI changes)
Current features:
  - addition, subtraction, mod, division, multiplication, int division
  - repeat loops
  - break points
  - print statements work too
  - eval expr using eval and checking for valid syms only
  - saving and loading of up to 10 diff files (by using keyboard lel)
  - if/else statements
  - debug mode (using breaks)
  - general variables
  - while loops
  - TODO:
   - convert everything to data
   - self-check on drawing game
   - super clear error handling
   - functions -> return values, confirm recursion works
   - range
   - confirm that debug-mode works correctly with fns
   - error handling <- uncomment all the excepts
"""

# Begin writing interpreter for Adaas <- saada
from tkinter import *
# from Modules.numericStringParser import NumericStringParser 

# taken from http://www.kosbie.net/cmu/fall-14/15-112/notes/file-and-web-io.py
def readFile(filename, mode="rt"):
    # rt = "read text"
    with open(filename, mode) as fin:
        return fin.read()

# taken from http://www.kosbie.net/cmu/fall-14/15-112/notes/file-and-web-io.py
def writeFile(filename, contents, mode="wt"):
    # wt = "write text"
    with open(filename, mode) as fout:
        fout.write(contents)

class Textbox(object):

    def __init__(self, x, y, width, height):
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.text = []
        self.clicked = False
        self.index=0

    def in_bounds(self, x, y):
        if ((self.x <= x <= self.x + self.width) and
                 (self.y <= y <= self.y + self.height)):
            self.clicked = True
            return True
        self.clicked = False
        return False
    
    def add_text(self, c):
        self.text.insert(self.index, c)
        self.index+=1

    def move_index(self, delta):
        if self.index + delta < 0 or self.index + delta >= len(self.text):
            return
        self.index += delta

    def backspace(self):
        if len(self.text) > 0:
            self.text.pop()

    def draw(self, canvas, counter=0):
        canvas.create_rectangle(self.x, self.y, 
                                self.x + self.width, 
                                self.y +self.height, 
                                width=3)
        margin = 10
        if counter % 10 < 5 and self.clicked:
            
            self.text.append("|")
            canvas.create_text(self.x + margin, self.y + margin, 
                text="".join(self.text), 
                anchor=NW, font="14")
            self.text.pop()
        else:
            canvas.create_text(self.x + margin, self.y + margin, 
                text="".join(self.text), 
                anchor=NW)

    def get_text(self):
        return "".join(self.text)

    def save(self, ver=None):
        contents = self.get_text()
        if ver == None:
            writeFile("code.txt", contents)
        else:
            filename = "code%d.txt" % ver
            writeFile(filename, contents)

    def load(self, ver=None):
        if ver == None:
            contents = readFile("code.txt")
        else:
            filename = "code%d.txt" % ver
            contents = readFile(filename)
        self.text = list(contents)
        self.index = len(self.text)

# helper function that replaces variable name in expression 
# with the variables actual value
# variables:dict<str, _> - maps variable names to vals
# expr:str - expression containing some or no var names
def replace_vars_with_values(data, variables, expr):
    
    elist = list(expr)
    offset = 0

    for i in range(len(expr)):
        if expr[i] == "%":
            elist.insert(i + offset, "%")
            offset += 1
    expr = "".join(elist)
    sorted_by_len = sorted(variables, key=lambda s: -len(s)) # longest first

    for var in sorted_by_len:
        if var in expr:
            num_occurences = expr.count(var)
            temp = expr.replace(var, "%r")
            expr = temp % ((variables[var],)*num_occurences)
    return expr

def test_replace_vars_with_vals(): 
    print("Testing replace_vars_with_vals...")
    class Struct(): pass
    data = Struct()
    variables = {"x": 10, "y":20, "z": 30, "color": "blue", "color2": None}
    assert(replace_vars_with_values(data, variables, "") == "")
    assert(replace_vars_with_values(data, variables, "x") == "10")
    assert(replace_vars_with_values(data, variables, "z") == "30")
    assert(replace_vars_with_values(data, variables, "color") == "\'blue\'")
    assert(replace_vars_with_values(data, variables, "x + y") == "10 + 20")
    assert(replace_vars_with_values(data, variables, "x + z*y") == "10 + 30*20")
    assert(replace_vars_with_values(data, variables, "color2") == 'None')
    assert(replace_vars_with_values(data, variables, "color color2") == 
                                                                    "\'blue\' None")
    assert(replace_vars_with_values(data, variables, "42") == "42")
    print("...passed!")

# helper function that processes mathematical assignment expressions
# variables is a mapping of var names to vals
# i is the line number of code being evaluated
# if expr doesn't result in a valid expr, None is returned and the error flag
#   is set
def eval_expr(data, variables, expr, line_num):
    assert(type(variables)==dict)
    if (expr == "") : return ""
    expr = replace_vars_with_values(data, variables, expr)
    print("165", expr)
    try:
        safe_syms = "<>/*+-.()%="
        for c in expr:
            if not(c.isspace() or c.isalpha() or c.isdigit() or c in safe_syms):   
                data.error = True
                data.err_msg = "math syntax error"
                data.err_line = line_num
                return None    
        if ("range" in expr): 
            return list(eval(expr))
        return eval(expr)
    except Exception as e:
        print("Exception line 178")
        print(expr)
        print(e)
        data.error = True
        data.err_msg = "math syntax error"
        data.err_line = line_num
        return None

def test_eval_expr():
    print("Testing eval_expr...")
    class Struct(): pass
    data = Struct()
    variables = {"x": 10, "y":20, "z": 30, "t": -5}
    assert(eval_expr(data, variables, "x + y * z / t", 0) == -110)
    assert(eval_expr(data, variables, "(x + x + x)*5", 0) == 150)
    assert(eval_expr(data, variables, "x**2 + y**2", 0) == 500)
    assert(eval_expr(data, variables, "0", 0) == 0)
    # assert(eval_expr(data, variables, "x+=0",0) == None) # == None)
    assert(eval_expr(data, variables, "(y+z)*(x+y)", 0) == 1500)
    assert(eval_expr(data, variables, "", 0) == "")
    print("...passed!")

# strip but doesn't strip the beginning because indents are important
def strip_end(s):
    slist = list(s)
    i = 0
    for i in range(len(s), 0, -1):
        if not(slist[i - 1].isspace()):
            break
    return "".join(slist[:i])

def test_strip_end():
    print("Testing strip_end...")
    assert(strip_end("") == "")
    assert(strip_end("asdfsdfsdf      ") == "asdfsdfsdf")
    assert(strip_end("asdf\t   \n \n") == "asdf")
    assert(strip_end("hi this is text with stuff\t \t \t ") == 
                     "hi this is text with stuff")
    assert(strip_end("\t    we don't touch stuff in the beginning  ") ==
                     "\t    we don't touch stuff in the beginning")
    assert(strip_end("how perfect there's nothing to strip") ==
                     "how perfect there's nothing to strip")
    print("...passed!")

# returns list of split lines and new line number
# extracts indented body of code as in body of for loop/if statement etc.
# returns list of lines of code as well as the new line number
def get_indent_body(data, code_lines, k):
    body = []
    while (k < len(code_lines) and code_lines[k][0].isspace()):
        start = code_lines[k][0]
        if start == " ":
            code_lines[k] = code_lines[k][4:] # get rid of 4 tabbed spaces
        elif start == "\t":
            code_lines[k] = code_lines[k][1:]
        else:
            print(code_lines[k])
            raise Exception("you missed something")
        body += [code_lines[k], "\n"] # there used to be strip_end call here
        k += 1
    return (body, k)

def test_get_indent_body():
    print("Testing get_indent_body...")
    class Struct(): pass
    data = Struct()
    code_lines = ["#this is code", "x<-10", "    x<-10"]
    assert(get_indent_body(data, code_lines, 2) == (["x<-10", "\n"], 3))
    assert(get_indent_body(data, code_lines, 1) == ([], 1))
    code_lines = ["repeat 5:", "    x<-x+10", "\ty<-y+10", "\tdraw", 
                  "# more stuff"]
    assert(get_indent_body(data, code_lines, 1) == 
                          (["x<-x+10", "\n", "y<-y+10", "\n", "draw", "\n"], 4))
    code_lines =  ["repeat 5:", "    x<-x+10", "\ty<-y+10", "\tdraw", 
                  "# more stuff", "\ty<-y+10", "\tdraw"]
    assert(get_indent_body(data ,code_lines, 5) == (["y<-y+10", "\n", "draw", "\n"], 7))
    print("...passed!")


# gets rid of empty lines, lines with comments
# inserts break points if debug mode is on
def filter_space(code_lines, debug=False):

    filtered_code = []
    for i in range(len(code_lines)):
        line = code_lines[i]
        if not(line == "" or line.isspace() or line.startswith("#")):
            filtered_code.append(strip_end(line))

    # adds break statements between lines
    if debug:
        result = [filtered_code[0]]
        for i in range(1, len(filtered_code)):
            debug_line = "break"
            line = filtered_code[i]
            if line[0].isspace():
                for j in range(len(line)):
                    if not(line[j].isspace()):
                        break
                debug_line = line[:j] + debug_line
            result.append(debug_line)
            result.append(filtered_code[i])
            if (i == len(filtered_code) - 1):
                result.append(debug_line)
    else:
        result = filtered_code
    return result

def test_filter_space():
    print("Testing filter_space...")
    code_lines = ["", "", "", "code", "code", "", "code", ""]
    code_lines1 = [" ", "    ", "        ", "code", "  ", "   ", "  ", "", 
                   "code"]
    code_lines2 = ["repeat 5:", "   ", "    x<-x+10", "\ty<-y+10", 
                                "\tdraw", "# more stuff", "\ty<-y+10", "\tdraw",
                                "", "# this is the end of stuff"]
    assert(filter_space(code_lines) == ["code", "code", "code"])
    assert(filter_space(code_lines1) == ["code", "code"])
    assert(filter_space(code_lines2) == ["repeat 5:", "    x<-x+10", "\ty<-y+10", 
                                "\tdraw", "\ty<-y+10", "\tdraw"])
    assert(filter_space(code_lines, True) == ["code", "break", "code", "break", 
                                        "code", "break"])
    assert(filter_space(code_lines2, True) == ['repeat 5:', '    break', 
        '    x<-x+10', '\tbreak', '\ty<-y+10', '\tbreak', '\tdraw', '\tbreak', 
        '\ty<-y+10', '\tbreak', '\tdraw', '\tbreak'])

def replace_functions_with_values(data, functions, variables, line, color, x0, y0, x1, y1, depth=0):

    sorted_by_len = sorted(functions, key=lambda s: -len(s)) # longest first
    for fn_name in sorted_by_len:
        i = line.find(fn_name)
        if (i != -1):
            lparen_i = line.find("(", i)
            rparen_i = line.find(")", i)
            name = line[:lparen_i].strip()
            vals = line[lparen_i+1:rparen_i]
            if vals == "":
                vals = []
            else:
                vals = [elem.strip() for elem in vals.split(",")] 
                vals = [eval_expr(data, variables, expr, i) for expr in vals]
            (_, args, body) = functions[fn_name]
            f_variables = dict()
            f_variables["x"] = variables["x"]
            f_variables["y"] = variables["y"]
            f_variables["color"] = variables["color"]
            for j in range(len(vals)):
                var_name = args[j]
                f_variables[var_name] = vals[j]
            # 0, color, 0, x0, y0, x1, y1
            (_, _, x0, y0, x1, y1, color, _) = interpret(data, body, f_variables, 0, color, 0, x0, y0, x1, y1, depth=depth+4)
            variables["x"] = f_variables["x"]
            variables["y"] = f_variables["y"]
            variables["color"] = f_variables["color"]
            line = (line[:i] + "%r" + line[rparen_i+1:]) % f_variables["return"]
            print("variables", variables)

    return line
"""
code: a multi-line string of "code"
variables: a dict mapping var name (str) to value
i: line number
color: string indicating color for line
repeated: number of iterations of for loop completed
x0, y0, x1, y1: coordinates for line
"""
def interpret(data, code, variables, i=0, color="", repeated=0, x0=0, y0=0, 
              x1=0, y1=0, depth=0):

    code_lines = code.splitlines()
    n = len(code_lines)
    print(" "*depth + "line 347 variables", variables)
    while (i < n):
        print("i: ", i)
        line = code_lines[i]
        print("processing line:", line)
        if line.startswith("x"):
            # if you can't split, then there's a syntax error
            try:
                expr = line.split("<-")[1].strip()
            except:
                data.error = True
                data.err_msg = "assignment should be of the form \'x <- 5\'"
                data.err_line = i
            expr = replace_functions_with_values(data, data.fns, variables, expr, color, x0, y0, x1, y1)
            val = eval_expr(data, variables, expr, i)
            if val == None:
                return None
            variables['x'] = val
            x1 = val

        elif line.startswith("y"):
            # if you can't split, then there's a syntax error
            expr = line.split("<-")[1].strip()
            expr = replace_functions_with_values(data, data.fns, variables, expr, color, x0, y0, x1, y1)
            val = eval_expr(data, variables, expr, i)
            if val == None: # there was an error
                return None 
            variables['y'] = val
            y1 = val

        elif line.startswith("color"):
            # if you can't split, then there's a syntax error
            if (len(line.split("<-")) < 2):
                data.error = True
                data.err_msg = "should be of the form \'color <- black\'"
                data.err_line = i
            color = line.split("<-")[1].strip() 
            variables["color"] = color
        
        elif "<-" in line:
            if (len(line.split("<-")) < 2):
                data.error = True
                data.err_msg = "should be of the form \'variable_name <- value\'"
                data.err_line = i

            (var, expr) = (line.split("<-")[0].strip(), line.split("<-")[1].strip())
            print("line 389 var, expr", var, expr)
            expr = replace_functions_with_values(data, data.fns, variables, expr, color, x0, y0, x1, y1)
            print("line 391", expr)
            res = eval_expr(data, variables, expr, i)
            if res == None:
                return 
            variables[var] = res

        elif line.startswith("draw"):
            if color != "none":
                print("x0, y0, x1, y1", (x0, y0, x1, y1))
                data.to_draw.append((x0, y0, x1, y1, color, i))
            x0 = x1
            y0 = y1
        
        elif line.startswith("if"):
            cond = None
            try: 
                # get contents between parens
                start = line.find("(")
                end = line.find(")")
                expr = line[start + 1:end]
                #DEFINE VAR AND VAL
                cond = eval_expr(data, variables, expr, i)

            except Exception as e:
                print("here!")
                data.error = True
                data.err_msg = str(e)
                print(e)
                data.err_line = i 

            if (cond == None): 
                data.error = True
                data.err_msg = str("Condition not set")
                data.err_line = i 
                return 
            (body, k) = get_indent_body(data, code_lines, i + 1)
            
            else_exists = False
            else_body = []
            if k < n and code_lines[k].startswith("else"):
                else_exists = True
                (else_body, k) = get_indent_body(data, code_lines, k + 1) 
            
            if (cond):
                result = interpret(data, "".join(body), variables, 0, color, 0, 
                    x0, y0, x1, y1, depth+1)

                if result == None and data.error: 
                    #error occured
                    return 
                (_, break_called, x0, y0, x1, y1, color, variables) = result
                if break_called:
                    # frame is line number, color, #repeats, and coord vals
                    frame = (variables, i, color, 0, x0, y0, x1, y1, color)
                    # print("appending frame: ", frame)
                    data.frames.append(frame)
                    return (False, True, x0, y0, x1, y1, color)
            elif (else_exists):
                result = interpret(data, "".join(else_body), variables, 
                                   0, color, 0, x0, y0, x1, y1, depth + 1)
                if result == None and data.error: #error occured
                    return 
                (_, break_called, x0, y0, x1, y1, color, variables) = result
                if break_called:
                    # frame is line number, color, #repeats, and coord vals
                    frame = (variables, i, color, 0, x0, y0, x1, y1)
                    print("appending frame: ", frame)
                    data.frames.append(frame)
                    return (False, True, x0, y0, x1, y1, color, variables)
            else:
                print("not cond")
            i = k - 1 # minus 1 because you increment i at the end
        elif line.startswith("while"):
            cond = None
            try: 
                # get contents between parens
                start = line.find("(")
                end = line.find(")")
                expr = line[start + 1:end]
                #DEFINE VAR AND VAL
                cond = eval_expr(data, variables, expr, i)

            except Exception as e:
                data.error = True
                data.err_msg = str(e)
                print(e)
                data.err_line = i 

            (body, k) = get_indent_body(data, code_lines, i + 1)

            while (cond or repeated):
                if data.frames != []:
                    result = interpret(data, "".join(body), *data.frames.pop())    
                else: 
                    result = interpret(data, "".join(body), variables, 0, color,
                                       0, x0, y0, x1, y1)
                
                if result == None and data.error: # error occured
                    return None

                (terminated, break_called, x0, y0, x1, y1, color, variables) = result
                cond = eval_expr(data, variables, expr, i)
                if break_called:
                    # terminated keeps track of whether you've completed 
                    # the inner code of the loop
                    if not cond and terminated: # this is the final iter of loop
                        i = k
                    frame = (variables, i, color, int(not(terminated)), x0, y0, x1, y1)
                    print("appending frame in while: ", frame)
                    data.frames.append(frame)
                    return (False, True, x0, y0, x1, y1, color, variables)

        elif line.startswith("repeat"): 
            # get the number between the end of repeat and before the colon
            try:
                expr = line.split(":")[0][len("repeat"):].strip()

            except Exception as e:
                data.error = True
                data.err_msg = str(e)
                data.err_line = i 
                return None

            if data.to_repeat == None:
                m = eval_expr(data, variables, expr, i)
                data.to_repeat = m
            
            print("before get_indent_body")
            print(code_lines)
            body, k = get_indent_body(data, code_lines, i + 1)
            print("repeat loop body:\n", "".join(body))
            # print(k)
            # never entered the while loop
            if k == i + 1: 
                data.error = True
                data.err_msg = "no content to repeat\n\
(Hint: remember to indent the block\nyou would like to repeat)"
                data.err_line = k 
                return None

            # data.to_repeat - repeated to handle for breakpoints
            for j in range(data.to_repeat - repeated):
    
                if data.frames != []:
                    result = interpret(data, "".join(body), *data.frames.pop(), depth+4)    
                else: 
                    print(" "*depth + "line 415 variables: ", variables)
                    print("x0, y0", x0, y0)
                    result = interpret(data, "".join(body), variables, 0, color, 0, x0, y0, x1, y1, depth+1)
                
                if result == None and data.error: # error occured
                    return 
                (terminated, break_called, x0, y0, x1, y1, color, variables) = result
                print(" "*depth + "line 421 variables", variables)
                if break_called:
                    
                    if j + 1 > data.to_repeat - repeated:
                        print("we should be exiting the loop")
                        i = k
                        data.to_repeat = None
    
                    # terminated keeps track of whether you've completed the inner loop or not
                    print("terminated: ", terminated)
                    if (terminated): repeated += 1
                    frame = (variables, i, color, repeated, x0, y0, x1, y1)
                    print("appending frame: ", frame)
                    data.frames.append(frame)
                    return (False, True, x0, y0, x1, y1, color, variables)
                
            data.to_repeat = None
            i = k - 1
            print("i = %d" % k)

        elif line.startswith("print"):
            s = line[len("print("):]
            end = s.find(")")
            s = s[:end]
            s = replace_vars_with_values(data, variables, s)
            data.print_string.append(s)

        elif line.startswith("break"):
            frame = (variables, i + 1, color, 0, x0, y0, x1, y1)
            
            if (i + 1) == n:
                return (True, True, x0, y0, x1, y1, color, variables)
            print("appending frame: ", frame)
            data.frames.append(frame)
            return (False, True, x0, y0, x1, y1, color, variables)

        elif line.startswith("def"):
            try:

                lparen_i = line.find("(") 
                rparen_i = line.find(")")
                offset = 4 # len("def ")
                fn_name = line[offset:lparen_i]
                args = line[lparen_i+1:rparen_i]
                args = [elem.strip() for elem in args.split(",")] # potential safety concern
                (body, k) = get_indent_body(data, code_lines, i + 1)
                i = k - 1 # set i to skip code for body
                # int, tuple, str
                data.fns[fn_name] = (len(args), args, "".join(body))
            except Exception as e:
                print(e)
                data.error = True
                data.err_msg = "\nfunction definition should be \
of the form\ndef function_name(argument1, argument2, argument3):\n\t#code content goes here"
                data.err_line = i
                return None
        # here is where you run the function        
        elif "(" in line:
            lparen_i = line.find("(")
            rparen_i = line.find(")")
            name = line[:lparen_i].strip()
            vals = line[lparen_i+1:rparen_i]
            if vals == "":
                vals = []
            else:
                vals = [elem.strip() for elem in vals.split(",")] 
                vals = [eval_expr(data, variables, expr, i) for expr in vals]
            if name in data.fns:
                (_, args, body) = data.fns[name]
                f_variables = dict()
                f_variables["x"] = variables["x"]
                f_variables["y"] = variables["y"]
                f_variables["color"] = variables["color"]
                for j in range(len(vals)):
                    var_name = args[j]
                    f_variables[var_name] = vals[j]
                # 0, color, 0, x0, y0, x1, y1
                (_, _, x0, y0, x1, y1, color, _) = interpret(data, body, f_variables, 0, color, 0, x0, y0, x1, y1, depth=depth+4)
                print("f_variables", f_variables)
                variables["x"] = f_variables["x"]
                variables["y"] = f_variables["y"]
                variables["color"] = f_variables["color"]
                print("x0: %d, y0: %d" % (x0, y0))
                print("variables", variables)
                print("returned")
            else:
                data.error = True
                data.err_msg = "invalid function name"
                data.err_line = i
                return None
            # replace any variable names with args with stuff
            # replace variables in body with values
            # ok what you need to do is you need to make the dict with the general variables
            # info, an arg so this is one of the things that can't be packed into data...
        elif line.startswith("return"):
            expr = line[len("return"):].strip()
            print(expr)
            variables["return"] = eval_expr(data, variables, expr, i)
            print("line 637: ret val" , variables["return"])

        else:
            # print some exception
            print("error:", repr(line))
            data.error = True
            data.err_msg = "invalid starting keyword"
            data.err_line = i 
            return None
    
        i += 1
    print(" "*depth +"line 495 variables: ", variables)
    return (True, False, x0, y0, x1, y1, color, variables)

def draw_code(canvas, data):

    draw_window_height = data.height - data.draw_window_margin*2
    cx = data.draw_window_margin + data.draw_window_width/2
    cy = data.draw_window_margin + draw_window_height/2
    for obj in data.to_draw:
        (x0, y0, x1, y1, color, i) = obj
        try:
            # negating y to account for conversion to graphical coordinates
            # to cartesian coordinates
            canvas.create_line(x0 + cx,
                               -y0 + cy, 
                               x1 + cx, 
                               -y1 + cy, fill=color, width=5)
        except Exception as e:
            data.error = True
            data.err_msg = str(e)
            data.err_line = i 

def draw_axes(canvas, data):

    
    offset = data.draw_window_margin
    draw_window_height = data.height - offset*2
    cx = data.draw_window_margin + data.draw_window_width/2
    cy = data.draw_window_margin + draw_window_height/2
    
    # x axis
    canvas.create_line(offset, 
                       cy,
                       offset + data.draw_window_width,
                       cy)

    # divide into chunks of 10 rounded down to the nearest even
    interval = 10
    increments_x = data.draw_window_width//interval//2*2 
    marker_radius = 5
    cx = data.draw_window_margin + data.draw_window_width/2
    cy = data.draw_window_margin + draw_window_height/2
    for i in range(1, int(increments_x//2 + 1)):
        mx = cx + interval*i
        mx_n = cx - interval*i
        my_top = cy - marker_radius
        my_bot = cy + marker_radius
        canvas.create_line(mx, my_top, mx, my_bot)
        canvas.create_line(mx_n, my_top, mx_n, my_bot)

    # draw y axis
    canvas.create_line(cx, data.draw_window_margin, cx, 
                       data.draw_window_margin + draw_window_height)

    increments_y = draw_window_height//interval//2*2
    for i in range(1, int(increments_y//2 + 1)):
        mx_l = cx - marker_radius
        mx_r = cx + marker_radius
        my = cy + interval*i
        my_n = cy - interval*i
        canvas.create_line(mx_l, my, mx_r, my)
        canvas.create_line(mx_l, my_n, mx_r, my_n)

def init_GUI(data):
    data.draw_window_margin = 20
    data.draw_window_width = data.width * 3/5
    data.draw_window_height = data.height - data.draw_window_margin*2
    data.margin = 10
    data.draw_window_cx = data.draw_window_margin + data.draw_window_width/2
    data.draw_window_cy = data.draw_window_margin + data.draw_window_height/2
    textx = data.draw_window_margin + data.draw_window_width + data.margin
    texty = data.draw_window_margin
    textw = (data.width - data.draw_window_width
             - data.draw_window_margin*2 - data.margin)
    texth = data.height - 2*data.draw_window_margin
    data.to_draw = []
    data.axes = False
    data.textbox = Textbox(textx, texty, textw, texth)

def init_compile_data(data):
    data.fns = dict()
    data.to_repeat = None 
    # data.variables = dict()
    # data.variables['x'] = 0
    # data.variables['y'] = 0
    # data.variables['color'] = None
    data.error = False
    data.err_msg = ""
    data.code = data.textbox.get_text()
    data.to_draw = []
    data.frames = []
    data.print_string = []

def init(data):
    init_GUI(data)
    init_compile_data(data)
    
    data.debug_mode = False
    data.type_mode = False
    data.counter = 0
     # each string in this list should be separated by new line
    data.help = False
    data.error = False
    data.ver = None
    data.err_line = 0
    data.err_msg = ""
    data.code = """x <- -25
y <- 25
color <- none
draw
y <- -25
color <- black
draw
x<--25
y<-0
draw
x<--5
draw
y<-25
color<-none
draw
y<--25
color<-black
draw
x<-5
y<-25
color<-none
draw
y<--25
color<-blue
draw
"""
    variables = dict()
    variables['x'] = 0
    variables['y'] = 0
    variables['color'] = None
    interpret(data, data.code, variables)

def mousePressed(event, data):
    # use event.x and event.y
    if data.textbox.in_bounds(event.x, event.y):
        data.type_mode = True
    else:
        data.type_mode = False

def keyPressed(event, data):
    # use event.char and event.keysym
    
    if data.type_mode:
        valid = "\><-+#:()^*/%=," 
        if event.keysym == "Return":
            data.textbox.add_text("\n")
        elif event.keysym == "Tab":
            data.textbox.add_text("    ")
        elif event.keysym == "BackSpace":
            data.textbox.backspace()
        elif (event.char.isalpha() 
             or event.char.isdigit() 
             or event.char.isspace()
             or event.char in valid):
            data.textbox.add_text(event.char)

    elif event.keysym == "Return":
        init_compile_data(data)
        code_lines = filter_space(data.code.splitlines(), data.debug_mode)
        data.code = "\n".join(code_lines)
        variables = dict()
        variables['x'] = 0
        variables['y'] = 0
        variables['color'] = None
        interpret(data, data.code, variables)
        print("frame post return: ", data.frames)

    # display axes
    elif event.char == "a":
        data.axes = not(data.axes)

    elif event.char == "d":
        data.debug_mode = not(data.debug_mode)

    elif event.char.isdigit():
        ver = int(event.char)
        if ver == 0:
            data.ver = None
        else:
            data.ver = int(event.char)
    # save code written
    elif event.char == "s":
        data.textbox.save(data.ver)
        print("data saved")

    # load previous code written
    elif event.char == "l":
        data.textbox.load(data.ver)
        print("data loaded")

    # help
    elif event.char == "h":
        data.help = True

    # continue after break point
    elif event.char == "c":
        while len(data.frames) > 0:
            frame = data.frames.pop()
            print("popped frame:", frame)
            # transfer results from recursive call for next iteration
            # variables = frame[-1]
            # frame = frame[:-1]

            result = interpret(data, data.code, *frame)
            if result == None: # an error occured
                data.frames = []
                return 
            (_, break_called, x0, y0, x1, y1, color, variables) = result
            if break_called:
                print("frame post c: ", data.frames)
                return


def timerFired(data):
    data.counter += 1

def draw_screens(canvas, data):
    canvas.create_rectangle(data.draw_window_margin, data.draw_window_margin,
                            data.draw_window_margin + data.draw_window_width, 
                            data.height - data.draw_window_margin, width=3)
    data.textbox.draw(canvas, data.counter)

def redrawAll(canvas, data):

    draw_screens(canvas, data)

    if data.debug_mode:
        canvas.create_text(data.draw_window_margin + data.margin,
                           data.draw_window_margin + data.draw_window_height,
                           text="Debug Mode",
                           anchor=SW)
    if data.axes:
        draw_axes(canvas, data)
    
    if data.error:
        err_msg = "Error on line %d: %s" % (data.err_line, data.err_msg)
        canvas.create_text(data.draw_window_margin + data.margin, 
                           data.draw_window_margin + data.margin, 
                           text=err_msg, anchor=NW)
    else:
        draw_code(canvas, data)
        p = "\n".join(data.print_string)
        canvas.create_text(data.draw_window_margin + data.margin,
                           data.draw_window_margin + data.margin,
                           text=p, anchor=NW)

def run(width=1000, height=600):
    def redrawAllWrapper(canvas, data):
        canvas.delete(ALL)
        redrawAll(canvas, data)
        canvas.update()    

    def mousePressedWrapper(event, canvas, data):
        mousePressed(event, data)
        redrawAllWrapper(canvas, data)

    def keyPressedWrapper(event, canvas, data):
        keyPressed(event, data)
        redrawAllWrapper(canvas, data)

    def timerFiredWrapper(canvas, data):
        timerFired(data)
        redrawAllWrapper(canvas, data)
        # pause, then call timerFired again
        canvas.after(data.timerDelay, timerFiredWrapper, canvas, data)
    # Set up data and call init
    class Struct(object): pass
    data = Struct()
    data.width = width
    data.height = height
    data.timerDelay = 100 # milliseconds
    init(data)
    # create the root and the canvas
    root = Tk()
    canvas = Canvas(root, width=data.width, height=data.height)
    canvas.pack()
    # set up events
    root.bind("<Button-1>", lambda event:
                            mousePressedWrapper(event, canvas, data))
    root.bind("<Key>", lambda event:
                            keyPressedWrapper(event, canvas, data))
    timerFiredWrapper(canvas, data)
    # and launch the app
    root.mainloop()  # blocks until window is closed
    print("bye!")

def test_all():
    print("Testing all functions...")
    test_replace_vars_with_vals()
    test_eval_expr()
    test_strip_end()
    test_get_indent_body()
    test_filter_space()
    print("...passed!")

test_all()
run(500, 400)