# age = 20
# if age >= 18:
#  	print("your age is ",age)
#  	print("adult")
# age = 3
# if age >= 18:
# 	print("your age is",age)
# 	print("adult")
# else:
# 	print("your age is",age)
# 	print("teanager")
# age_str = input("请输入您的年龄：")
# age = int(age_str)
# if age >= 18:
# 	print("adult")
# elif age >=6:
# 	print("teenager")
# else:
# 	print("kid")
# age = 28
# if age:
# 	print("teenager")
# elif age >= 18:
# 	print("adult")
# else:
# 	print("kid")
#-----------
# age_str = input("请输入您的年龄：")
# age = int(age_str)
# if age >= 18:
# 	print("adult")
# elif age >=6:
# 	print("teenager")
# else:
# 	print("kid")
#---------------
#-------test---------
print("使用BMI公式来测算您的健康程度！！")
height = float(input("请输入您的身高："))
weight = float(input("请输入您的体重："))
# height = 1.75
# weight = 60.5
BMI = weight / (height*height)
if BMI < 18.5:
	print("过轻")
elif BMI >= 18.5 and BMI < 25:
	print("正常")
elif BMI >= 25 and BMI < 28:
	print("过重")
elif BMI >= 28 and BMI < 32:
	print("肥胖")
else:
	print("严重肥胖")