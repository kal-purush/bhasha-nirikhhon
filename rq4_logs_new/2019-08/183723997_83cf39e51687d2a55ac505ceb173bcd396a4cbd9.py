from sys import argv
from CommandType import CommandType

class CodeWriter:
    def __init__(self, name):
        self.out = open(name.split(".")[0] + ".asm", "w")
        self.fn = ""
        # fn と合わせてloop labelを区別するためのindex
        self.lc = 0

    def setFileName(self, fileName):
        self.fn = fileName.split("/")[-1][:-3]
        self.lc = 0
        return
    # 二項演算のアセンブリ生成
    def gen_bin(self, op):
        # 1pop
        self.out.write("\t@SP\n")
        self.out.write("\tM=M-1\n")
        self.out.write("\tA=M\n")
        self.out.write("\tD=M\n")
        # 2pop
        self.out.write("\t@SP\n")
        self.out.write("\tM=M-1\n")
        self.out.write("\tA=M\n")
        # 処理
        self.out.write("\tD=M"+ op + "D\n")
        # push
        self.out.write("\t@SP\n")
        self.out.write("\tM=M+1\n")
        self.out.write("\tA=M-1\n")
        self.out.write("\tM=D\n")

        return
    # 比較
    def gen_cmp(self, op):
        self.out.write("//" + op + "\n")
        # 1pop
        self.out.write("\t@SP\n")
        self.out.write("\tM=M-1\n")
        self.out.write("\tA=M\n")
        self.out.write("\tD=M\n")
        # 2pop
        self.out.write("\t@SP\n")
        self.out.write("\tM=M-1\n")
        self.out.write("\tA=M\n")
        # 処理
        self.out.write("\tD=M-D\n")
        lid = self.fn + str(self.lc)
        self.lc += 1
        # trueなら-1にしてくれるところに飛ぶそうでなければ0
        self.out.write("\t@T" +lid + "\n")
        self.out.write("\tD;" + op+ "\n")
        self.out.write("\tD=0\n")
        self.out.write("\t@EC" +lid + "\n")
        self.out.write("\t0;JMP\n")
        # push
        #True処理
        self.out.write("(T" + lid + ")\n")
        self.out.write("\tD=-1\n")
        self.out.write("\t@EC" +lid + "\n")
        self.out.write("\t0;JMP\n")

        # end output
        self.out.write("\t(EC" +lid + ")\n")
        self.out.write("\t@SP\n")
        self.out.write("\tM=M+1\n")
        self.out.write("\tA=M-1\n")
        self.out.write("\tM=D\n")

        return
    # unary
    def gen_uni(self, op):
        # sp参照して処理
        self.out.write("//" + op + "\n")
        self.out.write("\t@SP\n")
        self.out.write("\tA=M-1\n")
        self.out.write("\tM=" + op + "M\n")

        return

    def writeArithmetic(self, command):
        if command == "add":
            self.out.write("//add\n")
            self.gen_bin("+")
        elif command == "sub":
            self.gen_bin("-")
        elif command == "neg":
            self.gen_uni("-")
        elif command == "eq":
            self.gen_cmp("JEQ")
        elif command == "gt":
            self.gen_cmp("JGT")
        elif command == "lt":
            self.gen_cmp("JLT")
        elif command == "and":
            self.gen_bin("&")
        elif command == "and":
            self.gen_bin("|")
        elif command == "not":
            self.gen_uni("!")
        return
    def writePushPop(self,command, segment, index):
        if command == CommandType.C_PUSH:
            self.out.write("//push\n")
            self.out.write("//まずindexずらす\n")
            # baseからindex分ずらす必要があるものは先にbaseをずらす(popしたデータを持ってたらDが使えないので)
            if segment in {"local", "argument", "this", "that"}:
                self.out.write("\t@" + str(index) + "\n")
                self.out.write("\tD=A\n")
                if segment == "local":
                    self.out.write("\t@LCL\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "argument":
                    self.out.write("\t@ARG\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "this":
                    self.out.write("\t@THIS\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "that":
                    self.out.write("\t@THAT\n")
                    self.out.write("\tM=M+D\n")
            # まずsegmentで参照元を決めて、データをとってくる
            # 定数ならその数字をそのまま
            self.out.write("//データとってくる\n")
            if segment == "constant":
                self.out.write("\t@" + str(index) + "\n")
            elif segment == "local":
                self.out.write("\t@LCL\n")
                self.out.write("\tA=M\n")
            elif segment == "argument":
                self.out.write("\t@ARG\n")
                self.out.write("\tA=M\n")
            elif segment == "this":
                self.out.write("\t@THIS\n")
                self.out.write("\tA=M\n")
            elif segment == "that":
                self.out.write("\t@THAT\n")
                self.out.write("\tA=M\n")
            elif segment == "pointer":
                targ = 3 + index
                self.out.write("\t@" + str(targ) + "\n")
            elif segment == "temp":
                targ = 5 + index
                self.out.write("\t@" + str(targ) + "\n")
            #Global変数
            elif segment == "static":
                self.out.write("\t@" + self.fn + str(index) +"\n")

            # 定数以外はアドレスをたどってデータをとる
            if segment == "constant":
                self.out.write("\tD=A\n")
            else:
                self.out.write("\tD=M\n")

            #stack pointer に値をセット
            self.out.write("\t@SP\n")
            self.out.write("\tA=M\n")
            self.out.write("\tM=D\n")
            #stack pointer をincrement
            self.out.write("\t@SP\n")
            self.out.write("\tM=M+1\n")
            self.out.write("//indexをもどす\n")
            # ずらしたbase addressを戻す
            if segment in {"local", "argument", "this", "that"}:
                self.out.write("\t@" + str(index) + "\n")
                self.out.write("\tD=A\n")
                if segment == "local":
                    self.out.write("\t@LCL\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "argument":
                    self.out.write("\t@ARG\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "this":
                    self.out.write("\t@THIS\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "that":
                    self.out.write("\t@THAT\n")
                    self.out.write("\tM=M-D\n")

        elif command == CommandType.C_POP:
            # baseからindex分ずらす必要があるものは先にbaseをずらす(popしたデータを持ってたらDが使えないので)
            self.out.write("//pop\n")
            self.out.write("//まずindexずらす\n")
            if segment in {"local", "argument", "this", "that"}:
                self.out.write("\t@" + str(index) + "\n")
                self.out.write("\tD=A\n")
                if segment == "local":
                    self.out.write("\t@LCL\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "argument":
                    self.out.write("\t@ARG\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "this":
                    self.out.write("\t@THIS\n")
                    self.out.write("\tM=M+D\n")
                elif segment == "that":
                    self.out.write("\t@THAT\n")
                    self.out.write("\tM=M+D\n")
            #stack pointer から値を取得, sp decrement
            self.out.write("\t@SP\n")
            self.out.write("\tM=M-1\n")
            self.out.write("\tA=M\n")
            self.out.write("\tD=M\n")
            # pop先Addressを参照する
            if segment == "constant":
                self.out.write("\t@" + str(index) + "\n")
            elif segment == "local":
                self.out.write("\t@LCL\n")
                self.out.write("\tA=M\n")
            elif segment == "argument":
                self.out.write("\t@ARG\n")
                self.out.write("\tA=M\n")
            elif segment == "this":
                self.out.write("\t@THIS\n")
                self.out.write("\tA=M\n")
            elif segment == "that":
                self.out.write("\t@THAT\n")
                self.out.write("\tA=M\n")
            elif segment == "pointer":
                targ = 3 + index
                self.out.write("\t@" + str(targ) + "\n")
            elif segment == "temp":
                targ = 5 + index
                self.out.write("\t@" + str(targ) + "\n")
            # 新しいGlobal変数
            elif segment == "static":
                self.out.write("\t@" + self.fn + str(index) +"\n")
            # 書き込み

            self.out.write("\tM=D\n")
            self.out.write("//indexをもどす\n")
            # ずらしたbase addressを戻す
            if segment in {"local", "argument", "this", "that"}:
                self.out.write("\t@" + str(index) + "\n")
                self.out.write("\tD=A\n")
                if segment == "local":
                    self.out.write("\t@LCL\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "argument":
                    self.out.write("\t@ARG\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "this":
                    self.out.write("\t@THIS\n")
                    self.out.write("\tM=M-D\n")
                elif segment == "that":
                    self.out.write("\t@THAT\n")
                    self.out.write("\tM=M-D\n")
        return
    def close(self):
        self.out.write("(END)\n")
        self.out.write("\t@END\n")
        self.out.write("\t0;JMP\n")
        self.out.close()
        return





if __name__ == "__main__":
    C = CodeWriter(argv[1])
    print(C.out)