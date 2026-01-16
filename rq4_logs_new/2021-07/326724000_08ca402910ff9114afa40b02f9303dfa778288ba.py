''''
Sub store : Inventory : Requisition | Create Requisition not working.

Create Requisition not working.

Navigation: Substore → Inventory → Inventory Requisition → Create Requisition
'''

from TestActionLibrary import A
from GlobalShareVariables import GSV

# front desk user login
StoreUserId = GSV.storeUserID
StoreUserPwd = GSV.storeUserPwD

#Declaring store values
Store1 = GSV.SubStore1
Inventory1 = GSV.Inventory1
ItemName1 = GSV.stationaryItem1
Qty1 = 3

IR = A()
IR.openBrowser()
IR.login(StoreUserId, StoreUserPwd)
IR.selectSubStore(substore=Store1)
IR.createSubStoreRequisition(InventoryName=Inventory1, ItemName=ItemName1, Qty=Qty1)