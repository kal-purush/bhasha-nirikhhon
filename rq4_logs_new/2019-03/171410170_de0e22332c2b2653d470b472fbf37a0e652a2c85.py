########################################
#
# @file EVlatentAnalysis.py
# @author: James Gong, Ali
# @date: 19 Feb 2019
#
#######################################

#######################################
from biogeme import *
from headers import *
from distributions import *
from loglikelihood import *
from statistics import *

############ Defined Variables #############################
VEHICLE_OPTION1_PRICE_SCALED = DefineVariable('VEHICLE_OPTION1_PRICE_SCALED', alt1pr / 100000.0)
VEHICLE_OPTION2_PRICE_SCALED = DefineVariable('VEHICLE_OPTION2_PRICE_SCALED', alt2pr / 100000.0)

Range_Scale_1= DefineVariable('Range_Scale_1', alt1rg / 100.0)
Range_Scale_2= DefineVariable('Range_Scale_2', alt2rg / 100.0)

R_time_Scale_1= DefineVariable('R_time_Scale_1', alt1ti / 10.0) 
R_time_Scale_2= DefineVariable('R_time_Scale_2', alt2ti / 10.0) 

Set_cost_Scale_1= DefineVariable('Set_cost_Scale_1', alt1se / 1000.0) 
Set_cost_Scale_2= DefineVariable('Set_cost_Scale_2', alt2se / 1000.0) 

Cost_km_Scale_1 = DefineVariable('Cost_km_Scale_1', alt1co / 10.0)
Cost_km_Scale_2 = DefineVariable('Cost_km_Scale_2', alt2co / 10.0)

Ava_fast_charge_Scale_1 = DefineVariable('Ava_fast_charge_Scale_1', alt1avl / 10.0)
Ava_fast_charge_Scale_2 = DefineVariable('Ava_fast_charge_Scale_2', alt2avl / 10.0)

Rebate_upfront_cost_Scale_1 = DefineVariable('Rebate_upfront_cost_Scale_1', alt1incup / 10000.0)
Rebate_upfront_cost_Scale_2 = DefineVariable('Rebate_upfront_cost_Scale_2', alt2incup / 10000.0)

Rebate_parking_Scale_1 = DefineVariable('Rebate_parking_Scale_1', alt1incprk / 100.0)
Rebate_parking_Scale_2 = DefineVariable('Rebate_parking_Scale_2', alt2incprk / 100.0)

Energy_bill_Scale_1 = DefineVariable('Energy_bill_Scale_1', alt1incbll / 100.0)
Energy_bill_Scale_2 = DefineVariable('Energy_bill_Scale_2', alt2incbll / 100.0)

Stamp_duty_Scale_1 = DefineVariable('Stamp_duty_Scale_1', alt1incst / 10.0)
Stamp_duty_Scale_2 = DefineVariable('Stamp_duty_Scale_2', alt2incst / 10.0)

Penetration_Scale_1 = DefineVariable('Penetration_Scale_1', alt1sld / 100.0)
Penetration_Scale_2 = DefineVariable('Penetration_Scale_2', alt2sld / 100.0)

'''
Small_Sedan_1 = DefineVariable('Small_Sedan_1', alt1vhtype==1)
Large_sedan_1= DefineVariable('Large_sedan_1', alt1vhtype==2)
Small_SUV_1= DefineVariable('Small_SUV_1', alt1vhtype==3)
Large_SUV_1= DefineVariable('Large_SUV_1', alt1vhtype==4)
Small_hatch_1= DefineVariable('Small_hatch_1', alt1vhtype==5)
Minivan_1= DefineVariable('Minivan_1', alt1vhtype==6)

Small_Sedan_2 = DefineVariable('Small_Sedan_2', alt2vhtype==1)
Large_sedan_2= DefineVariable('Large_sedan_2', alt2vhtype==2)
Small_SUV_2= DefineVariable('Small_SUV_2', alt2vhtype==3)
Large_SUV_2= DefineVariable('Large_SUV_2', alt2vhtype==4)
Small_hatch_2= DefineVariable('Small_hatch_2', alt2vhtype==5)
Minivan_2= DefineVariable('Minivan_2', alt2vhtype==6)
'''

######################################## Class A #############################################
ASC_OPTION1_A = Beta('ASC_OPTION1_A',-0.0925416,-100,100,1,'ASC_OPTION1_A' )
ASC_OPTION2_A = Beta('ASC_OPTION2_A',-0.126195,-100,100,1,'ASC_OPTION2_A' )
ASC_OPTION3_A = Beta('ASC_OPTION3_A',0,-100,100,1,'ASC_OPTION3_A' )

VEHICLE_PRICE_A = Beta('VEHICLE_PRICE_A',-5.15372,-100,100,1,'VEHICLE_PRICE_A' )
Range_A = Beta('Range_A',0.175471,-100,100,1,'Range_A' )
R_time_A = Beta('R_time_A',-0.629249,-100,100,1,'R_time_A' )
Set_cost_A = Beta('Set_cost_A',0,-100,100,1,'Set_cost_A' )
Cost_km_A = Beta('Cost_km_A',-0.243622,-100,100,1,'Cost_km_A' )
Ava_fast_A = Beta('Ava_fast_A',0,-100,100,1,'Ava_fast_A' )
Acc_bus_A = Beta('Acc_bus_A',0,-100,100,1,'Acc_bus_A' )
Rebate_upfront_cost_A = Beta('Rebate_upfront_cost_A',0.352822,-100,100,1,'Rebate_upfront_cost_A' )
Rebate_parking_A = Beta('Rebate_parking_A',0,-100,100,1,'Rebate_parking_A' )
Energy_bill_A = Beta('Energy_bill_A',0.560112,-100,100,1,'Energy_bill_A' )
Stamp_duty_A = Beta('Stamp_duty_A',-0.155771,-100,100,1,'Stamp_duty_A' )
Penetration_A = Beta('Penetration_A',0,-100,100,1,'Penetration_A' )
Small_Sedan_A = Beta('Small_Sedan_A',1.69762,-100,100,1,'Small_Sedan_A' )
Large_sedan_A = Beta('Large_sedan_A',0.334826,-100,100,1,'Large_sedan_A' )
Small_SUV_A = Beta('Small_SUV_A',0.992947,-100,100,1,'Small_SUV_A' )
Large_SUV_A = Beta('Large_SUV_A',0,-100,100,1,'Large_SUV_A' )
Small_hatch_A = Beta('Small_hatch_A',1.7199,-100,100,1,'Small_hatch_A' )


# Utility functions
V1A = ASC_OPTION1_A + VEHICLE_PRICE_A * VEHICLE_OPTION1_PRICE_SCALED + Range_A*Range_Scale_1 + R_time_A*R_time_Scale_1 + Set_cost_A*Set_cost_Scale_1 + Cost_km_A*Cost_km_Scale_1 + Ava_fast_A*Ava_fast_charge_Scale_1 + Acc_bus_A*alt1acc + Rebate_upfront_cost_A*Rebate_upfront_cost_Scale_1 + Rebate_parking_A*Rebate_parking_Scale_1 + Energy_bill_A*Energy_bill_Scale_1 + Stamp_duty_A*Stamp_duty_Scale_1 + Penetration_A*Penetration_Scale_1 + Small_Sedan_A*(alt1vhtype==1) + Large_sedan_A*(alt1vhtype==2) + Small_SUV_A*(alt1vhtype==3) + Large_SUV_A*(alt1vhtype==4) + Small_hatch_A*(alt1vhtype==5) 
V2A = ASC_OPTION2_A + VEHICLE_PRICE_A * VEHICLE_OPTION2_PRICE_SCALED+ Range_A*Range_Scale_2 + R_time_A*R_time_Scale_2 + Set_cost_A*Set_cost_Scale_2 + Cost_km_A*Cost_km_Scale_2 + Ava_fast_A*Ava_fast_charge_Scale_2 + Acc_bus_A*alt2acc + Rebate_upfront_cost_A*Rebate_upfront_cost_Scale_2 + Rebate_parking_A*Rebate_parking_Scale_2 + Energy_bill_A*Energy_bill_Scale_2 + Stamp_duty_A*Stamp_duty_Scale_2 + Penetration_A*Penetration_Scale_2 + Small_Sedan_A*(alt2vhtype==1) + Large_sedan_A*(alt2vhtype==2) + Small_SUV_A*(alt2vhtype==3) + Large_SUV_A*(alt2vhtype==4) + Small_hatch_A*(alt2vhtype==5) 
V3A = ASC_OPTION3_A

# Associate utility functions with the numbering of alternatives
VA = {
    1: V1A,
    2: V2A,
    3: V3A}


avA = {
    1: 1,
    2: 1,
    3: 1}


# The choice model is a logit, with availability conditions
prob_A = bioLogit(VA, avA, choice)


########################### Class B ###################################################

ASC_OPTION1_B = Beta('ASC_OPTION1_B',-5.18618,-100,100,1,'ASC_OPTION1_B' )
ASC_OPTION2_B = Beta('ASC_OPTION2_B',-5.3422,-100,100,1,'ASC_OPTION2_B' )
ASC_OPTION3_B = Beta('ASC_OPTION3_B',0,-100,100,1,'ASC_OPTION3_B' )

VEHICLE_PRICE_B = Beta('VEHICLE_PRICE_B',-1.89822,-100,100,1,'VEHICLE_PRICE_B' )
Range_B = Beta('Range_B',0.402235,-100,100,1,'Range_B' )
R_time_B = Beta('R_time_B',-2.39115,-100,100,1,'R_time_B' )
Set_cost_B = Beta('Set_cost_B',0,-100,100,1,'Set_cost_B' )
Cost_km_B = Beta('Cost_km_B',0,-100,100,1,'Cost_km_B' )
Ava_fast_B = Beta('Ava_fast_B',0,-100,100,1,'Ava_fast_B' )
Acc_bus_B = Beta('Acc_bus_B',-0.917205,-100,100,1,'Acc_bus_B' )
Rebate_upfront_cost_B = Beta('Rebate_upfront_cost_B',0,-100,100,1,'Rebate_upfront_cost_B' )
Rebate_parking_B = Beta('Rebate_parking_B',0,-100,100,1,'Rebate_parking_B' )
Energy_bill_B = Beta('Energy_bill_B',1.18031,-100,100,1,'Energy_bill_B' )
Stamp_duty_B = Beta('Stamp_duty_B',0.391661,-100,100,1,'Stamp_duty_B' )
Penetration_B = Beta('Penetration_B',0,-100,100,1,'Penetration_B' )
Small_Sedan_B = Beta('Small_Sedan_B',1.19202,-100,100,1,'Small_Sedan_B' )
Large_sedan_B = Beta('Large_sedan_B',1.54696,-100,100,1,'Large_sedan_B' )
Small_SUV_B = Beta('Small_SUV_B',0,-100,100,1,'Small_SUV_B' )
Large_SUV_B = Beta('Large_SUV_B',0,-100,100,1,'Large_SUV_B' )
Small_hatch_B = Beta('Small_hatch_B',1.13876,-100,100,1,'Small_hatch_B' )

# Utility functions
V1B = ASC_OPTION1_B + VEHICLE_PRICE_B * VEHICLE_OPTION1_PRICE_SCALED + Range_B*Range_Scale_1 + R_time_B*R_time_Scale_1 + Set_cost_B*Set_cost_Scale_1 + Cost_km_B*Cost_km_Scale_1 + Ava_fast_B*Ava_fast_charge_Scale_1 + Acc_bus_B*alt1acc + Rebate_upfront_cost_B*Rebate_upfront_cost_Scale_1 + Rebate_parking_B*Rebate_parking_Scale_1 + Energy_bill_B*Energy_bill_Scale_1 + Stamp_duty_B*Stamp_duty_Scale_1 + Penetration_B*Penetration_Scale_1+ Small_Sedan_B*(alt1vhtype==1) + Large_sedan_B*(alt1vhtype==2) + Small_SUV_B*(alt1vhtype==3) + Large_SUV_B*(alt1vhtype==4) + Small_hatch_B*(alt1vhtype==5) 
V2B = ASC_OPTION2_B + VEHICLE_PRICE_B * VEHICLE_OPTION2_PRICE_SCALED + Range_B*Range_Scale_2 + R_time_B*R_time_Scale_2 + Set_cost_B*Set_cost_Scale_2 + Cost_km_B*Cost_km_Scale_2 + Ava_fast_B*Ava_fast_charge_Scale_2 + Acc_bus_B*alt2acc + Rebate_upfront_cost_B*Rebate_upfront_cost_Scale_2 + Rebate_parking_B*Rebate_parking_Scale_2 + Energy_bill_B*Energy_bill_Scale_2 + Stamp_duty_B*Stamp_duty_Scale_2 + Penetration_B*Penetration_Scale_2+ Small_Sedan_B*(alt2vhtype==1) + Large_sedan_B*(alt2vhtype==2) + Small_SUV_B*(alt2vhtype==3) + Large_SUV_B*(alt2vhtype==4) + Small_hatch_B*(alt2vhtype==5) 
V3B = ASC_OPTION3_B

# Associate utility functions with the numbering of alternatives
VB = {
    1: V1B,
    2: V2B,
    3: V3B}

avB = {
    1: 1,
    2: 1,
    3: 1}


# The choice model is a logit, with availability conditions
prob_B = bioLogit(VB, avB, choice)

########################### Class C ###################################################

ASC_OPTION1_C = Beta('ASC_OPTION1_C',-1.29417,-100,100,1,'ASC_OPTION1_C' )
ASC_OPTION2_C = Beta('ASC_OPTION2_C',-1.20641,-100,100,1,'ASC_OPTION2_C' )
ASC_OPTION3_C = Beta('ASC_OPTION3_C',0,-100,100,1,'ASC_OPTION3_C' )

VEHICLE_PRICE_C = Beta('VEHICLE_PRICE_C',-2.65299,-100,100,1,'VEHICLE_PRICE_C' )
Range_C = Beta('Range_C',0.22129,-100,100,1,'Range_C' )
R_time_C = Beta('R_time_C',-1.31282,-100,100,1,'R_time_C' )
Set_cost_C = Beta('Set_cost_C',0,-100,100,1,'Set_cost_C' )
Cost_km_C = Beta('Cost_km_C',-0.864849,-100,100,1,'Cost_km_C' )
Ava_fast_C = Beta('Ava_fast_C',0,-100,100,1,'Ava_fast_C' )
Acc_bus_C = Beta('Acc_bus_C',0,-100,100,1,'Acc_bus_C' )
Rebate_upfront_cost_C = Beta('Rebate_upfront_cost_C',0.742948,-100,100,1,'Rebate_upfront_cost_C' )
Rebate_parking_C = Beta('Rebate_parking_C',0.0701713,-100,100,1,'Rebate_parking_C' )
Energy_bill_C = Beta('Energy_bill_C',1.19448,-100,100,1,'Energy_bill_C' )
Stamp_duty_C = Beta('Stamp_duty_C',0,-100,100,1,'Stamp_duty_C' )
Penetration_C = Beta('Penetration_C',0.77929,-100,100,1,'Penetration_C' )
Small_Sedan_C = Beta('Small_Sedan_C',0.647292,-100,100,1,'Small_Sedan_C' )
Large_sedan_C = Beta('Large_sedan_C',1.56983,-100,100,1,'Large_sedan_C' )
Small_SUV_C = Beta('Small_SUV_C',2.91772,-100,100,1,'Small_SUV_C' )
Large_SUV_C = Beta('Large_SUV_C',3.01394,-100,100,1,'Large_SUV_C' )
Small_hatch_C = Beta('Small_hatch_C',0.842589,-100,100,1,'Small_hatch_C' )

# Utility functions
V1C = ASC_OPTION1_C + VEHICLE_PRICE_C * VEHICLE_OPTION1_PRICE_SCALED + Range_C*Range_Scale_1 + R_time_C*R_time_Scale_1 + Set_cost_C*Set_cost_Scale_1 + Cost_km_C*Cost_km_Scale_1 + Ava_fast_C*Ava_fast_charge_Scale_1 + Acc_bus_C*alt1acc + Rebate_upfront_cost_C*Rebate_upfront_cost_Scale_1 + Rebate_parking_C*Rebate_parking_Scale_1 + Energy_bill_C*Energy_bill_Scale_1 + Stamp_duty_C*Stamp_duty_Scale_1 + Penetration_C*Penetration_Scale_1+ Small_Sedan_C*(alt1vhtype==1) + Large_sedan_C*(alt1vhtype==2) + Small_SUV_C*(alt1vhtype==3) + Large_SUV_C*(alt1vhtype==4) + Small_hatch_C*(alt1vhtype==5) 
V2C = ASC_OPTION2_C + VEHICLE_PRICE_C * VEHICLE_OPTION2_PRICE_SCALED + Range_C*Range_Scale_2 + R_time_C*R_time_Scale_2 + Set_cost_C*Set_cost_Scale_2 + Cost_km_C*Cost_km_Scale_2 + Ava_fast_C*Ava_fast_charge_Scale_2 + Acc_bus_C*alt2acc + Rebate_upfront_cost_C*Rebate_upfront_cost_Scale_2 + Rebate_parking_C*Rebate_parking_Scale_2 + Energy_bill_C*Energy_bill_Scale_2 + Stamp_duty_C*Stamp_duty_Scale_2 + Penetration_C*Penetration_Scale_2+ Small_Sedan_C*(alt2vhtype==1) + Large_sedan_C*(alt2vhtype==2) + Small_SUV_C*(alt2vhtype==3) + Large_SUV_C*(alt2vhtype==4) + Small_hatch_C*(alt2vhtype==5) 
V3C = ASC_OPTION3_C

# Associate utility functions with the numbering of alternatives
VC = {
    1: V1C,
    2: V2C,
    3: V3C}

avC = {
    1: 1,
    2: 1,
    3: 1}


# The choice model is a logit, with availability conditions
prob_C = bioLogit(VC, avC, choice)

########################### Class D ###################################################
ASC_OPTION1_D = Beta('ASC_OPTION1_D',2.78981,-100,100,1,'ASC_OPTION1_D' )
ASC_OPTION2_D = Beta('ASC_OPTION2_D',2.91259,-100,100,1,'ASC_OPTION2_D' )
ASC_OPTION3_D = Beta('ASC_OPTION3_D',0,-100,100,1,'ASC_OPTION3_D' )

VEHICLE_PRICE_D = Beta('VEHICLE_PRICE_D',-0.934465,-100,100,1,'VEHICLE_PRICE_D' )
Range_D = Beta('Range_D',0.101453,-100,100,1,'Range_D' )
R_time_D = Beta('R_time_D',-0.284727,-100,100,1,'R_time_D' )
Set_cost_D = Beta('Set_cost_D',0,-100,100,1,'Set_cost_D' )
Cost_km_D = Beta('Cost_km_D',-0.408255,-100,100,1,'Cost_km_D' )
Ava_fast_D = Beta('Ava_fast_D',0,-100,100,1,'Ava_fast_D' )
Acc_bus_D = Beta('Acc_bus_D',0,-100,100,1,'Acc_bus_D' )
Rebate_upfront_cost_D = Beta('Rebate_upfront_cost_D',0.19945,-100,100,1,'Rebate_upfront_cost_D' )
Rebate_parking_D = Beta('Rebate_parking_D',0,-100,100,1,'Rebate_parking_D' )
Energy_bill_D = Beta('Energy_bill_D',0.26521,-100,100,1,'Energy_bill_D' )
Stamp_duty_D = Beta('Stamp_duty_D',0,-100,100,1,'Stamp_duty_D' )
Penetration_D = Beta('Penetration_D',0.422227,-100,100,1,'Penetration_D' )
Small_Sedan_D = Beta('Small_Sedan_D',0.189961,-100,100,1,'Small_Sedan_D' )
Large_sedan_D = Beta('Large_sedan_D',0,-100,100,1,'Large_sedan_D' )
Small_SUV_D = Beta('Small_SUV_D',0.193698,-100,100,1,'Small_SUV_D' )
Large_SUV_D = Beta('Large_SUV_D',0,-100,100,1,'Large_SUV_D' )
Small_hatch_D = Beta('Small_hatch_D',0.284696,-100,100,1,'Small_hatch_D' )

# Utility functions
V1D = ASC_OPTION1_D + VEHICLE_PRICE_D * VEHICLE_OPTION1_PRICE_SCALED + Range_D*Range_Scale_1 + R_time_D*R_time_Scale_1 + Set_cost_D*Set_cost_Scale_1 + Cost_km_D*Cost_km_Scale_1 + Ava_fast_D*Ava_fast_charge_Scale_1 + Acc_bus_D*alt1acc + Rebate_upfront_cost_D*Rebate_upfront_cost_Scale_1 + Rebate_parking_D*Rebate_parking_Scale_1 + Energy_bill_D*Energy_bill_Scale_1 + Stamp_duty_D*Stamp_duty_Scale_1 + Penetration_D*Penetration_Scale_1+ Small_Sedan_D*(alt1vhtype==1) + Large_sedan_D*(alt1vhtype==2) + Small_SUV_D*(alt1vhtype==3) + Large_SUV_D*(alt1vhtype==4) + Small_hatch_D*(alt1vhtype==5) 
V2D = ASC_OPTION2_D + VEHICLE_PRICE_D * VEHICLE_OPTION2_PRICE_SCALED + Range_D*Range_Scale_2 + R_time_D*R_time_Scale_2 + Set_cost_D*Set_cost_Scale_2 + Cost_km_D*Cost_km_Scale_2 + Ava_fast_D*Ava_fast_charge_Scale_2 + Acc_bus_D*alt2acc + Rebate_upfront_cost_D*Rebate_upfront_cost_Scale_2 + Rebate_parking_D*Rebate_parking_Scale_2 + Energy_bill_D*Energy_bill_Scale_2 + Stamp_duty_D*Stamp_duty_Scale_2 + Penetration_D*Penetration_Scale_2+ Small_Sedan_D*(alt2vhtype==1) + Large_sedan_D*(alt2vhtype==2) + Small_SUV_D*(alt2vhtype==3) + Large_SUV_D*(alt2vhtype==4) + Small_hatch_D*(alt2vhtype==5) 
V3D = ASC_OPTION3_D

# Associate utility functions with the numbering of alternatives
VD = {
    1: V1D,
    2: V2D,
    3: V3D}

avD = {
    1: 1,
    2: 1,
    3: 1}


# The choice model is a logit, with availability conditions
prob_D = bioLogit(VD, avD, choice)

########################### Class E ###################################################
ASC_OPTION1_E = Beta('ASC_OPTION1_E',0,-100,100,0,'ASC_OPTION1_E' )
ASC_OPTION2_E = Beta('ASC_OPTION2_E',0,-100,100,0,'ASC_OPTION2_E' )
ASC_OPTION3_E = Beta('ASC_OPTION3_E',0,-100,100,1,'ASC_OPTION3_E' )

VEHICLE_PRICE_E = Beta('VEHICLE_PRICE_E',0,-100,100,0,'VEHICLE_PRICE_E' )
Range_E = Beta('Range_E', 0,-100,100,0,'Range_E' )
R_time_E = Beta('R_time_E',0,-100,100,0,'R_time_E' )
Set_cost_E = Beta('Set_cost_E',0,-100,100,0,'Set_cost_E' )
Cost_km_E = Beta('Cost_km_E',0,-100,100,0,'Cost_km_E' )
Ava_fast_E = Beta('Ava_fast_E',0,-100,100,0,'Ava_fast_E' )
Acc_bus_E = Beta('Acc_bus_E',0,-100,100,0,'Acc_bus_E' )
Rebate_upfront_cost_E = Beta('Rebate_upfront_cost_E',0,-100,100,0,'Rebate_upfront_cost_E' )
Rebate_parking_E = Beta('Rebate_parking_E',0,-100,100,0,'Rebate_parking_E' )
Energy_bill_E = Beta('Energy_bill_E',0,-100,100,0,'Energy_bill_E' )
Stamp_duty_E = Beta('Stamp_duty_E',0,-100,100,0,'Stamp_duty_E' )
Penetration_E = Beta('Penetration_E',0,-100,100,0,'Penetration_E' )
Small_Sedan_E = Beta('Small_Sedan_E',0,-100,100,0,'Small_Sedan_E' )
Large_sedan_E = Beta('Large_sedan_E',0,-100,100,0,'Large_sedan_E' )
Small_SUV_E = Beta('Small_SUV_E',0,-100,100,0,'Small_SUV_E' )
Large_SUV_E = Beta('Large_SUV_E',0,-100,100,0,'Large_SUV_E' )
Small_hatch_E = Beta('Small_hatch_E',0,-100,100,0,'Small_hatch_E' )

# Utility functions
V1E = ASC_OPTION1_E + VEHICLE_PRICE_E * VEHICLE_OPTION1_PRICE_SCALED + Range_E*Range_Scale_1 + R_time_E*R_time_Scale_1 + Set_cost_E*Set_cost_Scale_1 + Cost_km_E*Cost_km_Scale_1 + Ava_fast_E*Ava_fast_charge_Scale_1 + Acc_bus_E*alt1acc + Rebate_upfront_cost_E*Rebate_upfront_cost_Scale_1 + Rebate_parking_E*Rebate_parking_Scale_1 + Energy_bill_E*Energy_bill_Scale_1 + Stamp_duty_E*Stamp_duty_Scale_1 + Penetration_E*Penetration_Scale_1+ Small_Sedan_E*(alt1vhtype==1) + Large_sedan_E*(alt1vhtype==2) + Small_SUV_E*(alt1vhtype==3) + Large_SUV_E*(alt1vhtype==4) + Small_hatch_E*(alt1vhtype==5) 
V2E = ASC_OPTION2_E + VEHICLE_PRICE_E * VEHICLE_OPTION2_PRICE_SCALED + Range_E*Range_Scale_2 + R_time_E*R_time_Scale_2 + Set_cost_E*Set_cost_Scale_2 + Cost_km_E*Cost_km_Scale_2 + Ava_fast_E*Ava_fast_charge_Scale_2 + Acc_bus_E*alt2acc + Rebate_upfront_cost_E*Rebate_upfront_cost_Scale_2 + Rebate_parking_E*Rebate_parking_Scale_2 + Energy_bill_E*Energy_bill_Scale_2 + Stamp_duty_E*Stamp_duty_Scale_2 + Penetration_E*Penetration_Scale_2+ Small_Sedan_E*(alt2vhtype==1) + Large_sedan_E*(alt2vhtype==2) + Small_SUV_E*(alt2vhtype==3) + Large_SUV_E*(alt2vhtype==4) + Small_hatch_E*(alt2vhtype==5) 
V3E = ASC_OPTION3_E

# Associate utility functions with the numbering of alternatives
VE = {
    1: V1E,
    2: V2E,
    3: V3E}

avE = {
    1: 1,
    2: 1,
    3: 1}


# The choice model is a logit, with availability conditions
prob_E = bioLogit(VE, avE, choice)

################################### CLASS MEMBERSHIP ############################################
#age_66_85_A = Beta ('age_66_85_A',0,-100,100,1) (Base)
#not_sure_A = Beta ('not_sure_A',0,-100,100,1) (Base)
#full_autonomy_nh_A = Beta ('full_autonomy_nh_A',0,-100,100,1) (Base)
#other_hh_A = Beta ('other_hh_A',0,-100,100,1)(Base)
#incom_nosay_A = Beta ('incom_nosay_A',0,-100,100,1)(base)
#retired_A = Beta ('retired_A',0,-100,100,1)(base)
#no_certificate_A = Beta ('no_certificate_A',0,-100,100,1)(base)
#other_home_A = Beta ('other_home_A',0,-100,100,1)(base)
#other_A = Beta ('other_A',0,-100,100,1)(base)

##########################################CLASS A
class_A = Beta('class_A',-0.166584,-100,100,0,'class_A' )
age_A = Beta('age_A',0,-100,100,1,'age_A' )
age_18_30_A = Beta('age_18_30_A',-1.78633,-100,100,1,'age_18_30_A' )
age_31_45_A = Beta('age_31_45_A',-0.926331,-100,100,1,'age_31_45_A' )
age_46_65_A = Beta('age_46_65_A',0,-100,100,1,'age_46_65_A' )
brand_new_A = Beta('brand_new_A',-0.347249,-100,100,1,'brand_new_A' )
second_hand_A = Beta('second_hand_A',0,-100,100,1,'second_hand_A' )
familiar_EV_A = Beta('familiar_EV_A',-0.316415,-100,100,1,'familiar_EV_A' )
EV_common_q5_9_A = Beta('EV_common_q5_9_A',0,-100,100,1,'EV_common_q5_9_A' )
affordable_q5_10_1_A = Beta('affordable_q5_10_1_A',0.849845,-100,100,1,'affordable_q5_10_1_A' )
longer_range_q5_10_2_A = Beta('longer_range_q5_10_2_A',0,-100,100,1,'longer_range_q5_10_2_A' )
infrastructure_q5_10_3_A = Beta('infrastructure_q5_10_3_A',0,-100,100,1,'infrastructure_q5_10_3_A' )
type_ev_q5_10_6_A = Beta('type_ev_q5_10_6_A',0,-100,100,1,'type_ev_q5_10_6_A' )
incentive_q5_11_5_A = Beta('incentive_q5_11_5_A',0,-100,100,1,'incentive_q5_11_5_A' )
human_only_A = Beta('human_only_A',0,-100,100,1,'human_only_A' )
modern_vehicle_A = Beta('modern_vehicle_A',0,-100,100,1,'modern_vehicle_A' )
modern_plus_A = Beta('modern_plus_A',0,-100,100,1,'modern_plus_A' )
partial_autonomy_A = Beta('partial_autonomy_A',0,-100,100,1,'partial_autonomy_A' )
full_autonomy_h_A = Beta('full_autonomy_h_A',0,-100,100,1,'full_autonomy_h_A' )
support_ban_A = Beta('support_ban_A',-0.457636,-100,100,1,'support_ban_A' )
female_A = Beta('female_A',0,-100,100,1,'female_A' )
couple_no_kid_A = Beta('couple_no_kid_A',0,-100,100,1,'couple_no_kid_A' )
couple_kid_A = Beta('couple_kid_A',-0.642818,-100,100,1,'couple_kid_A' )
one_parent_A = Beta('one_parent_A',0,-100,100,1,'one_parent_A' )
single_hh_A = Beta('single_hh_A',0.605052,-100,100,1,'single_hh_A' )
income_cat_A = Beta('income_cat_A',0.0616889,-100,100,1,'income_cat_A' )
incom_belo_52_A = Beta('incom_belo_52_A',0,-100,100,1,'incom_belo_52_A' )
incom_belo_104_A = Beta('incom_belo_104_A',0,-100,100,1,'incom_belo_104_A' )
incom_more_104_A = Beta('incom_more_104_A',0,-100,100,1,'incom_more_104_A' )
fulltime_emp_A = Beta('fulltime_emp_A',-0.653672,-100,100,1,'fulltime_emp_A' )
parttime_emp_A = Beta('parttime_emp_A',0,-100,100,1,'parttime_emp_A' )
unemployed_A = Beta('unemployed_A',0,-100,100,1,'unemployed_A' )
graduate_A = Beta('graduate_A',0,-100,100,1,'graduate_A' )
undergrad_A = Beta('undergrad_A',0,-100,100,1,'undergrad_A' )
certificate_A = Beta('certificate_A',0,-100,100,1,'certificate_A' )
house_A = Beta('house_A',0,-100,100,1,'house_A' )
apartment_A = Beta('apartment_A',0,-100,100,1,'apartment_A' )
owned_out_A = Beta('owned_out_A',0,-100,100,1,'owned_out_A' )
owend_mort_A = Beta('owend_mort_A',0,-100,100,1,'owend_mort_A' )
rent_A = Beta('rent_A',0,-100,100,1,'rent_A' )

##########################################CLASS B
class_B = Beta('class_B',0.121378,-100,100,0,'class_B' )
age_B = Beta('age_B',0,-100,100,1,'age_B' )
age_18_30_B = Beta('age_18_30_B',-1.78741,-100,100,1,'age_18_30_B' )
age_31_45_B = Beta('age_31_45_B',-1.12663,-100,100,1,'age_31_45_B' )
age_46_65_B = Beta('age_46_65_B',0,-100,100,1,'age_46_65_B' )
brand_new_B = Beta('brand_new_B',0,-100,100,1,'brand_new_B' )
second_hand_B = Beta('second_hand_B',0,-100,100,1,'second_hand_B' )
familiar_EV_B = Beta('familiar_EV_B',-0.579858,-100,100,1,'familiar_EV_B' )
EV_common_q5_9_B = Beta('EV_common_q5_9_B',0.0318423,-100,100,1,'EV_common_q5_9_B' )
affordable_q5_10_1_B = Beta('affordable_q5_10_1_B',-1.13689,-100,100,1,'affordable_q5_10_1_B' )
longer_range_q5_10_2_B = Beta('longer_range_q5_10_2_B',0,-100,100,1,'longer_range_q5_10_2_B' )
infrastructure_q5_10_3_B = Beta('infrastructure_q5_10_3_B',0,-100,100,1,'infrastructure_q5_10_3_B' )
type_ev_q5_10_6_B = Beta('type_ev_q5_10_6_B',0,-100,100,1,'type_ev_q5_10_6_B' )
incentive_q5_11_5_B = Beta('incentive_q5_11_5_B',0,-100,100,1,'incentive_q5_11_5_B' )
human_only_B = Beta('human_only_B',1.11854,-100,100,1,'human_only_B' )
modern_vehicle_B = Beta('modern_vehicle_B',0.411211,-100,100,1,'modern_vehicle_B' )
modern_plus_B = Beta('modern_plus_B',0,-100,100,1,'modern_plus_B' )
partial_autonomy_B = Beta('partial_autonomy_B',0,-100,100,1,'partial_autonomy_B' )
full_autonomy_h_B = Beta('full_autonomy_h_B',0,-100,100,1,'full_autonomy_h_B' )
support_ban_B = Beta('support_ban_B',-1.41759,-100,100,1,'support_ban_B' )
female_B = Beta('female_B',0.465161,-100,100,1,'female_B' )
couple_no_kid_B = Beta('couple_no_kid_B',0,-100,100,1,'couple_no_kid_B' )
couple_kid_B = Beta('couple_kid_B',0,-100,100,1,'couple_kid_B' )
one_parent_B = Beta('one_parent_B',0,-100,100,1,'one_parent_B' )
single_hh_B = Beta('single_hh_B',0.811825,-100,100,1,'single_hh_B' )
income_cat_B = Beta('income_cat_B',0.108289,-100,100,1,'income_cat_B' )
incom_belo_52_B = Beta('incom_belo_52_B',0,-100,100,1,'incom_belo_52_B' )
incom_belo_104_B = Beta('incom_belo_104_B',0,-100,100,1,'incom_belo_104_B' )
incom_more_104_B = Beta('incom_more_104_B',0,-100,100,1,'incom_more_104_B' )
fulltime_emp_B = Beta('fulltime_emp_B',-1.01621,-100,100,1,'fulltime_emp_B' )
parttime_emp_B = Beta('parttime_emp_B',-0.440081,-100,100,1,'parttime_emp_B' )
unemployed_B = Beta('unemployed_B',0,-100,100,1,'unemployed_B' )
graduate_B = Beta('graduate_B',-0.872764,-100,100,1,'graduate_B' )
undergrad_B = Beta('undergrad_B',-0.63057,-100,100,1,'undergrad_B' )
certificate_B = Beta('certificate_B',0,-100,100,1,'certificate_B' )
house_B = Beta('house_B',0,-100,100,1,'house_B' )
apartment_B = Beta('apartment_B',0,-100,100,1,'apartment_B' )
owned_out_B = Beta('owned_out_B',0,-100,100,1,'owned_out_B' )
owend_mort_B = Beta('owend_mort_B',-0.5514,-100,100,1,'owend_mort_B' )
rent_B = Beta('rent_B',0,-100,100,1,'rent_B' )

##########################################CLASS C
class_C = Beta('class_C',-0.491785,-100,100,0,'class_C' )
age_C = Beta('age_C',0,-100,100,1,'age_C' )
age_18_30_C = Beta('age_18_30_C',0,-100,100,1,'age_18_30_C' )
age_31_45_C = Beta('age_31_45_C',0,-100,100,1,'age_31_45_C' )
age_46_65_C = Beta('age_46_65_C',0,-100,100,1,'age_46_65_C' )
brand_new_C = Beta('brand_new_C',0,-100,100,1,'brand_new_C' )
second_hand_C = Beta('second_hand_C',-0.857057,-100,100,1,'second_hand_C' )
familiar_EV_C = Beta('familiar_EV_C',0,-100,100,1,'familiar_EV_C' )
EV_common_q5_9_C = Beta('EV_common_q5_9_C',0,-100,100,1,'EV_common_q5_9_C' )
affordable_q5_10_1_C = Beta('affordable_q5_10_1_C',0,-100,100,1,'affordable_q5_10_1_C' )
longer_range_q5_10_2_C = Beta('longer_range_q5_10_2_C',0,-100,100,1,'longer_range_q5_10_2_C' )
infrastructure_q5_10_3_C = Beta('infrastructure_q5_10_3_C',0.818077,-100,100,1,'infrastructure_q5_10_3_C' )
type_ev_q5_10_6_C = Beta('type_ev_q5_10_6_C',0,-100,100,1,'type_ev_q5_10_6_C' )
incentive_q5_11_5_C = Beta('incentive_q5_11_5_C',0,-100,100,1,'incentive_q5_11_5_C' )
human_only_C = Beta('human_only_C',0,-100,100,1,'human_only_C' )
modern_vehicle_C = Beta('modern_vehicle_C',0,-100,100,1,'modern_vehicle_C' )
modern_plus_C = Beta('modern_plus_C',0,-100,100,1,'modern_plus_C' )
partial_autonomy_C = Beta('partial_autonomy_C',0,-100,100,1,'partial_autonomy_C' )
full_autonomy_h_C = Beta('full_autonomy_h_C',0,-100,100,1,'full_autonomy_h_C' )
support_ban_C = Beta('support_ban_C',0,-100,100,1,'support_ban_C' )
female_C = Beta('female_C',0,-100,100,1,'female_C' )
couple_no_kid_C = Beta('couple_no_kid_C',0,-100,100,1,'couple_no_kid_C' )
couple_kid_C = Beta('couple_kid_C',0,-100,100,1,'couple_kid_C' )
one_parent_C = Beta('one_parent_C',1.07071,-100,100,1,'one_parent_C' )
single_hh_C = Beta('single_hh_C',0,-100,100,1,'single_hh_C' )
income_cat_C = Beta('income_cat_C',0,-100,100,1,'income_cat_C' )
incom_belo_52_C = Beta('incom_belo_52_C',-1.98406,-100,100,1,'incom_belo_52_C' )
incom_belo_104_C = Beta('incom_belo_104_C',-0.543587,-100,100,1,'incom_belo_104_C' )
incom_more_104_C = Beta('incom_more_104_C',-0.555648,-100,100,1,'incom_more_104_C' )
fulltime_emp_C = Beta('fulltime_emp_C',-0.900718,-100,100,1,'fulltime_emp_C' )
parttime_emp_C = Beta('parttime_emp_C',0,-100,100,1,'parttime_emp_C' )
unemployed_C = Beta('unemployed_C',0,-100,100,1,'unemployed_C' )
graduate_C = Beta('graduate_C',0,-100,100,1,'graduate_C' )
undergrad_C = Beta('undergrad_C',0,-100,100,1,'undergrad_C' )
certificate_C = Beta('certificate_C',0,-100,100,1,'certificate_C' )
house_C = Beta('house_C',0,-100,100,1,'house_C' )
apartment_C = Beta('apartment_C',0,-100,100,1,'apartment_C' )
owned_out_C = Beta('owned_out_C',0,-100,100,1,'owned_out_C' )
owend_mort_C = Beta('owend_mort_C',0,-100,100,1,'owend_mort_C' )
rent_C = Beta('rent_C',0,-100,100,1,'rent_C' )

##########################################CLASS D
class_D = Beta('class_D', 0,-100,100,0,'class_D' )
age_D = Beta('age_D',0,-100,100,0,'age_D' )
age_18_30_D = Beta('age_18_30_D',0,-100,100,0,'age_18_30_D' )
age_31_45_D = Beta('age_31_45_D',0,-100,100,0,'age_31_45_D' )
age_46_65_D = Beta('age_46_65_D',0,-100,100,0,'age_46_65_D' )
brand_new_D = Beta('brand_new_D',0,-100,100,0,'brand_new_D' )
second_hand_D = Beta('second_hand_D',0,-100,100,0,'second_hand_D' )
familiar_EV_D = Beta('familiar_EV_D',0,-100,100,0,'familiar_EV_D' )
EV_common_q5_9_D = Beta('EV_common_q5_9_D',0,-100,100,0,'EV_common_q5_9_D' )
affordable_q5_10_1_D = Beta('affordable_q5_10_1_D',0,-100,100,0,'affordable_q5_10_1_D' )
longer_range_q5_10_2_D = Beta('longer_range_q5_10_2_D',0,-100,100,0,'longer_range_q5_10_2_D' )
infrastructure_q5_10_3_D = Beta('infrastructure_q5_10_3_D',0,-100,100,0,'infrastructure_q5_10_3_D' )
type_ev_q5_10_6_D = Beta('type_ev_q5_10_6_D',0,-100,100,0,'type_ev_q5_10_6_D' )
incentive_q5_11_5_D = Beta('incentive_q5_11_5_D',0,-100,100,0,'incentive_q5_11_5_D' )
human_only_D = Beta('human_only_D',0,-100,100,0,'human_only_D' )
modern_vehicle_D = Beta('modern_vehicle_D',0,-100,100,0,'modern_vehicle_D' )
modern_plus_D = Beta('modern_plus_D',0,-100,100,0,'modern_plus_D' )
partial_autonomy_D = Beta('partial_autonomy_D',0,-100,100,0,'partial_autonomy_D' )
full_autonomy_h_D = Beta('full_autonomy_h_D',0,-100,100,0,'full_autonomy_h_D' )
support_ban_D = Beta('support_ban_D',0,-100,100,0,'support_ban_D' )
female_D = Beta('female_D',0,-100,100,0,'female_D' )
couple_no_kid_D = Beta('couple_no_kid_D',0,-100,100,0,'couple_no_kid_D' )
couple_kid_D = Beta('couple_kid_D',0,-100,100,0,'couple_kid_D' )
one_parent_D = Beta('one_parent_D',0,-100,100,0,'one_parent_D' )
single_hh_D = Beta('single_hh_D',0,-100,100,0,'single_hh_D' )
income_cat_D = Beta('income_cat_D',0,-100,100,0,'income_cat_D' )
incom_belo_52_D = Beta('incom_belo_52_D',0,-100,100,0,'incom_belo_52_D' )
incom_belo_104_D = Beta('incom_belo_104_D',0,-100,100,0,'incom_belo_104_D' )
incom_more_104_D = Beta('incom_more_104_D',0,-100,100,0,'incom_more_104_D' )
fulltime_emp_D = Beta('fulltime_emp_D',0,-100,100,0,'fulltime_emp_D' )
parttime_emp_D = Beta('parttime_emp_D',0,-100,100,0,'parttime_emp_D' )
unemployed_D = Beta('unemployed_D',0,-100,100,0,'unemployed_D' )
graduate_D = Beta('graduate_D',0,-100,100,0,'graduate_D' )
undergrad_D = Beta('undergrad_D',0,-100,100,0,'undergrad_D' )
certificate_D = Beta('certificate_D',0,-100,100,0,'certificate_D' )
house_D = Beta('house_D',0,-100,100,0,'house_D' )
apartment_D = Beta('apartment_D',0,-100,100,0,'apartment_D' )
owned_out_D = Beta('owned_out_D',0,-100,100,0,'owned_out_D' )
owend_mort_D = Beta('owend_mort_D',0,-100,100,0,'owend_mort_D' )
rent_D = Beta('rent_C',0,-100,100,0,'rent_D' )

##########################################CLASS E
class_E = Beta('class_E',0,-100,100,1,'class_E' )

# age = beta()
prob1 = class_A + age_A*age + age_18_30_A*age_18_30 + age_31_45_A*age_31_45 + age_46_65_A*age_46_65 + brand_new_A*brand_new + second_hand_A*second_hand + familiar_EV_A*familiar_EV + EV_common_q5_9_A*EV_common_q5_9 + affordable_q5_10_1_A*affordable_q5_10_1 + longer_range_q5_10_2_A*longer_range_q5_10_2 + infrastructure_q5_10_3_A*infrastructure_q5_10_3 + type_ev_q5_10_6_A*type_ev_q5_10_6 + incentive_q5_11_5_A*incentive_q5_11_5 + human_only_A*human_only + modern_vehicle_A*modern_vehicle + modern_plus_A*modern_plus + partial_autonomy_A*partial_autonomy + full_autonomy_h_A*full_autonomy_h + support_ban_A*support_ban + female_A*female + couple_no_kid_A*couple_no_kid + couple_kid_A*couple_kid + one_parent_A*one_parent + single_hh_A*single_hh + income_cat_A*income_cat + incom_belo_52_A*incom_belo_52 + incom_belo_104_A*incom_belo_104 + incom_more_104_A*incom_more_104 + fulltime_emp_A*fulltime_emp + parttime_emp_A*parttime_emp + unemployed_A*unemployed + graduate_A*graduate + undergrad_A*undergrad + certificate_A*certificate + house_A*house + apartment_A*apartment + owned_out_A*owned_out + owend_mort_A*owend_mort + rent_A*rent
prob2 = class_B + age_B*age + age_18_30_B*age_18_30 + age_31_45_B*age_31_45 + age_46_65_B*age_46_65 + brand_new_B*brand_new + second_hand_B*second_hand + familiar_EV_B*familiar_EV + EV_common_q5_9_B*EV_common_q5_9 + affordable_q5_10_1_B*affordable_q5_10_1 + longer_range_q5_10_2_B*longer_range_q5_10_2 + infrastructure_q5_10_3_B*infrastructure_q5_10_3 + type_ev_q5_10_6_B*type_ev_q5_10_6 + incentive_q5_11_5_B*incentive_q5_11_5 + human_only_B*human_only + modern_vehicle_B*modern_vehicle + modern_plus_B*modern_plus + partial_autonomy_B*partial_autonomy + full_autonomy_h_B*full_autonomy_h + support_ban_B*support_ban + female_B*female + couple_no_kid_B*couple_no_kid + couple_kid_B*couple_kid + one_parent_B*one_parent + single_hh_B*single_hh + income_cat_B*income_cat + incom_belo_52_B*incom_belo_52 + incom_belo_104_B*incom_belo_104 + incom_more_104_B*incom_more_104 + fulltime_emp_B*fulltime_emp + parttime_emp_B*parttime_emp + unemployed_B*unemployed + graduate_B*graduate + undergrad_B*undergrad + certificate_B*certificate + house_B*house + apartment_B*apartment + owned_out_B*owned_out + owend_mort_B*owend_mort + rent_B*rent
prob3 = class_C + age_C*age + age_18_30_C*age_18_30 + age_31_45_C*age_31_45 + age_46_65_C*age_46_65 + brand_new_C*brand_new + second_hand_C*second_hand + familiar_EV_C*familiar_EV + EV_common_q5_9_C*EV_common_q5_9 + affordable_q5_10_1_C*affordable_q5_10_1 + longer_range_q5_10_2_C*longer_range_q5_10_2 + infrastructure_q5_10_3_C*infrastructure_q5_10_3 + type_ev_q5_10_6_C*type_ev_q5_10_6 + incentive_q5_11_5_C*incentive_q5_11_5 + human_only_C*human_only + modern_vehicle_C*modern_vehicle + modern_plus_C*modern_plus + partial_autonomy_C*partial_autonomy + full_autonomy_h_C*full_autonomy_h + support_ban_C*support_ban + female_C*female + couple_no_kid_C*couple_no_kid + couple_kid_C*couple_kid + one_parent_C*one_parent + single_hh_C*single_hh + income_cat_C*income_cat + incom_belo_52_C*incom_belo_52 + incom_belo_104_C*incom_belo_104 + incom_more_104_C*incom_more_104 + fulltime_emp_C*fulltime_emp + parttime_emp_C*parttime_emp + unemployed_C*unemployed + graduate_C*graduate + undergrad_C*undergrad + certificate_C*certificate + house_C*house + apartment_C*apartment + owned_out_C*owned_out + owend_mort_C*owend_mort + rent_C*rent
prob4 = class_D + age_D*age + age_18_30_D*age_18_30 + age_31_45_D*age_31_45 + age_46_65_D*age_46_65 + brand_new_D*brand_new + second_hand_D*second_hand + familiar_EV_D*familiar_EV + EV_common_q5_9_D*EV_common_q5_9 + affordable_q5_10_1_D*affordable_q5_10_1 + longer_range_q5_10_2_D*longer_range_q5_10_2 + infrastructure_q5_10_3_D*infrastructure_q5_10_3 + type_ev_q5_10_6_D*type_ev_q5_10_6 + incentive_q5_11_5_D*incentive_q5_11_5 + human_only_D*human_only + modern_vehicle_D*modern_vehicle + modern_plus_D*modern_plus + partial_autonomy_D*partial_autonomy + full_autonomy_h_D*full_autonomy_h + support_ban_D*support_ban + female_D*female + couple_no_kid_D*couple_no_kid + couple_kid_D*couple_kid + one_parent_D*one_parent + single_hh_D*single_hh + income_cat_D*income_cat + incom_belo_52_D*incom_belo_52 + incom_belo_104_D*incom_belo_104 + incom_more_104_D*incom_more_104 + fulltime_emp_D*fulltime_emp + parttime_emp_D*parttime_emp + unemployed_D*unemployed + graduate_D*graduate + undergrad_D*undergrad + certificate_D*certificate + house_D*house + apartment_D*apartment + owned_out_D*owned_out + owend_mort_D*owend_mort + rent_D*rent
prob5 = class_E

probClass1 = exp(prob1)/(exp(prob1) + exp(prob2) + exp(prob3) + exp(prob4) + exp(prob5))
probClass2 = exp(prob2)/(exp(prob1) + exp(prob2) + exp(prob3) + exp(prob4) + exp(prob5))
probClass3 = exp(prob3)/(exp(prob1) + exp(prob2) + exp(prob3) + exp(prob4) + exp(prob5))
probClass4 = exp(prob4)/(exp(prob1) + exp(prob2) + exp(prob3) + exp(prob4) + exp(prob5))
probClass5 = exp(prob5)/(exp(prob1) + exp(prob2) + exp(prob3) + exp(prob4) + exp(prob5))
# probClass2 = 1 - probClass1

probClass11 = Sum(probClass1, 'panelObsIter')/Sum(1, 'panelObsIter')
probClass22 = Sum(probClass2, 'panelObsIter')/Sum(1, 'panelObsIter')
probClass33 = Sum(probClass3, 'panelObsIter')/Sum(1, 'panelObsIter')
probClass44 = Sum(probClass4, 'panelObsIter')/Sum(1, 'panelObsIter')
probClass55 = Sum(probClass5, 'panelObsIter')/Sum(1, 'panelObsIter')


# id starts from 1 to 1076
metaIterator('personIter', '__dataFile__','panelObsIter', 'id')

# Defines an itertor on the data
rowIterator('panelObsIter', 'personIter')

#Conditional probability for the sequence of choices of an individual
condProbIndiv1 = Prod(prob_A,'panelObsIter')
condProbIndiv2 = Prod(prob_B,'panelObsIter')
condProbIndiv3 = Prod(prob_C,'panelObsIter')
condProbIndiv4 = Prod(prob_D,'panelObsIter')
condProbIndiv5 = Prod(prob_E,'panelObsIter')

 
# probability of individual belonging to a class
probIndiv = (probClass11*condProbIndiv1) + (probClass22*condProbIndiv2) + (probClass33*condProbIndiv3) + (probClass44*condProbIndiv4) + (probClass55*condProbIndiv5)

# DEfine the likelihood function for the estimation 
# Likelihood function
loglikelihood = Sum(log(probIndiv),'personIter')
BIOGEME_OBJECT.ESTIMATE = loglikelihood

### if you want to run simulation make sure that the "BIOGEME_OBJECT.ESTIMATE" function is disabled.
 
'''
simulate = {'id':id,
			'probClass1':probClass1,
			'probClass2':probClass2,
			'probClass3':probClass3,
			'probClass4':probClass4,
			'prob_A':prob_A,
			'prob_B':prob_B,
			'prob_C':prob_C,
			'prob_D':prob_D,
			'prob1':prob1,
			'prob2':prob2,
			'prob3':prob3,
			'prob4':prob4,
			'V1A':V1A,
			'V2A':V2A,
			'V1B':V1B,
			'V2B':V2B,
			'V1C':V1C,
			'V2C':V2C,
			'V1D':V1D,
			'V2D':V2D}

BIOGEME_OBJECT.SIMULATE = Enumerate(simulate,'panelObsIter')
'''


BIOGEME_OBJECT.PARAMETERS['optimizationAlgorithm'] = "CFSQP"
BIOGEME_OBJECT.PARAMETERS['checkDerivatives'] = "0" #0 is faster
BIOGEME_OBJECT.PARAMETERS['numberOfThreads'] = "38"