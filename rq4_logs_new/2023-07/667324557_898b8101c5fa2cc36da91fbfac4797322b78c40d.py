class BasicPlan:
	can_stream = True
	can_download = True
	has_SD = True
	has_HD = False
	has_UHD = False
	num_of_devices = 1
	price = "$8.99"
	
class StandardPlan(BasicPlan):
    has_HD = True
    num_of_devices = 2
    price = "$12.99"

class PremiumPlan(BasicPlan):
    has_UHD = False
    num_of_devices = 4
    price = "$15.99"


print(BasicPlan.has_SD)
print(PremiumPlan.has_SD)
print(BasicPlan.has_UHD)
print(BasicPlan.price)
print(PremiumPlan.num_of_devices)