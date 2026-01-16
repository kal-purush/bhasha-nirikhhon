
# 자바에서 다중상속은 인터페이스(= 추상메소드만을 가지고 있는 클래스 - > 추상메소드 그대로 쓰면 컴파일x 링크 과정에서 에러가 발생한다.)

# 다이아몬드 형태로 클래스를 상속했을 떄 , 함수가 중복된다면 문제가 생긴다. 

#class A : 처럼 D의 형태 
#def __init__(self):
#print("A 생성자 호출")
#class B(A) :
#def __init__(self):
#A.__init__(self)
#print("B 생성자 호출")
#class C(A) :
#def __init__(self):
#A.__init__(self)
#print("C 생성자 호출")
#class D(B,C):
#def __init__(self):
#B.__init__(self)
#C.__init__(self)
#print("D 생성자 호출")

# super.__init__() 으로 중복을 방지할 수 있다. super 는 부모에 대한 생성자를 반환한다. 

#from abc import ABCMeta, abstractmethod
#class Terran(metaclass=ABCMeta):
#    def __init__(self, name):
#        self.name = name

#    @abstractmethod
#    def attack(self):
#        pass




#class Tank(Terran):
#    def __init(self,name,damage):
#        super().__init__(name)
#        self.damage = damage

#    def attack(self):
#        print("탱크를 쏩니다.")


#class Marine(Terran):
#    def __init__(self,name,hp):
#        super().__init__(name)
#        self.hp = hp

#    def attack(self):
#        print("총을 쏩니다.")

#def Attack(Terran):
#    Terran.attack()

#t1 = Tank("Tank",0)
#t2 = Marine("Marine",50)

#tlist = [Tank("Tank1",0),Tank("Tank2",0),Marine("Marine1",50),Marine("Marine2",50)]

#for item in tlist:
#    Attack(item)


class MyList(list):
    name=""
    def __init__(self,name):
        super().__init__([]) # 빈 리스트로 초기화
        self.name = name

    def print(self):
        for i in self:
            print(i)

    def __str__(self):
        return self.name + " : " + super().__str__()


list1 = MyList("greenjoa")
list1.append(10)
list1.append(20)
list1.append(30)
list1.append(40)
list1.append(50)

print(list1)

# object + 10  -> __add__ 재정의
# 10 + object -> __radd__라는 함수를 재정의 