import sys
import pandas as pd

def get_mortgate_pmt(principal, irate, term):
    irate = irate/1200  # -> percentage rate / 12 since monthly payment
    return float(principal) * float((irate*(1+irate)**term) / (((1+irate)**term)-1))

def calc_expenses(rent, vac_rate, maint_rate, prop_mgmt_fees, taxes, insurance):
    return (rent * vac_rate) \
              + (rent * maint_rate) \
              + (rent * prop_mgmt_fees) \
              + (taxes / 12) \
              + (insurance / 12)

def calc_cash_flow(rent, vac_rate, maint_rate, prop_mgmt_fees, taxes, insurance, mgt_payment):
    expenses = calc_expenses(rent, vac_rate, maint_rate, prop_mgmt_fees, taxes, insurance)
    return rent - expenses - mgt_payment

""" Maximum Principal for this property to ensure cash flow >= min_cf """
def calc_max_principal(rent, vac_rate, maint_rate, prop_mgmt_fees, taxes, insurance, \
                       irate, term, min_cf=100):
    irate = irate/1200
    expenses = calc_expenses(rent, vac_rate, maint_rate, prop_mgmt_fees, taxes, insurance)
    qty = float((irate*(1+irate)**term) / (((1+irate)**term)-1))
    p = (-min_cf + rent - expenses) / qty
    return p/.8

def read_data(filename):
    return pd.read_csv(filename, sep=r'\s*,\s*', encoding='ascii', engine='python')

def prompt_data():
    purchase_price = float(input("\nPurchase Price:\t\t\t$"))
    rent = float(input("Estimated Rent:\t\t\t$"))
    closing_costs = float(input("Estimated Closing Costs:\t$"))
    loan_amt = input("Loan Amount (Enter for default):$")
    if loan_amt == '' or loan_amt is None:
        loan_amt = purchase_price * .8
    else:
        loan_amt = float(loan_amt)
    irate = float(input("Interest Rate (%):\t\t"))
    term = float(input("Loan Term (months):\t\t"))

    print("")

    vac = float(input("Vacancy Rate (%):\t\t")) / 100
    maint = float(input("Maintenance (% of rent):\t")) / 100
    pmf = float(input("Prop. Mgmt. Fees (% of rent):\t")) / 100
    txs = float(input("Annual Prop. Taxes:\t\t$"))
    ins = float(input("Annual Prop. Insurance:\t\t$"))

    data = {
        "ID": [1],
        "Purchase Price": [purchase_price],
        "Rental Income" : [rent],
        "Closing Costs" : [closing_costs],
        "Loan Amount" : [loan_amt],
        "Interest Rate" : [irate],
        "Vacancy Rate" : [vac],
        "Loan Term" : [term],
        "Maintenance Expense" : [maint],
        "Property Management Fees" : [pmf],
        "Annual Property Taxes" : [txs],
        "Annual Property Insurance" : [ins]
    }
    return pd.DataFrame(data)

if __name__ == '__main__':
    data = None
    output_filename = "results.csv"
    for i in range(len(sys.argv)):
        if "--csv" in sys.argv[i]:
            data = read_data(sys.argv[i+1])
        if "--outfile" in sys.argv[i]:
            output_filename = sys.argv[i+1]
    data = prompt_data() if data is None else data

    results = {
        "ID": [],
        "Mortgage Payment" : [],
        "Total Payments": [],
        "Total Interest": [],
        "Cash Required": [],
        "Minimum Monthly Expenses": [],
        "Monthly Cash Flow": [],
        "Max Purhcase Price": [],
        "Annual Yield (CoC ROI)": [],
        "Cap Rate": []
    }

    for i in range(data.shape[0]):
        row = data.iloc[i]
        
        pmt = get_mortgate_pmt(row["Purchase Price"], row["Interest Rate"], row["Loan Term"])
        results["ID"].append(row["ID"])
        results["Mortgage Payment"].append(round(pmt, 2))
        results["Total Payments"].append(round(pmt*row["Loan Term"], 2))
        results["Total Interest"].append(round((pmt*row["Loan Term"] - row["Loan Amount"]), 2))

        upfront_cash = row["Closing Costs"] + (row["Purchase Price"] - row["Loan Amount"])
        results["Cash Required"].append(round(upfront_cash, 2))
        
        expenses = pmt + (row["Annual Property Taxes"]/12) + (row["Annual Property Insurance"]/12)      # exludes maintenance, prop. management, and vacancy
        results["Minimum Monthly Expenses"].append(round(expenses, 2))
        
        cf = calc_cash_flow(row["Rental Income"], row["Vacancy Rate"], row["Maintenance Expense"], row["Property Management Fees"], 
            row["Annual Property Taxes"], row["Annual Property Insurance"], pmt)
        results["Monthly Cash Flow"].append(round(cf, 2))

        max_purchase = calc_max_principal(row["Rental Income"], row["Vacancy Rate"], row["Maintenance Expense"], row["Property Management Fees"], 
            row["Annual Property Taxes"], row["Annual Property Insurance"], row["Interest Rate"], row["Loan Term"])
        results["Max Purhcase Price"].append(round(max_purchase, 2))

        cap_rate = ((cf+pmt)*12)/row["Purchase Price"] ## cap rate = what if we bought the property with cash?
        results["Annual Yield (CoC ROI)"].append(round(((12*cf)/upfront_cash)*100, 2))
        results["Cap Rate"].append(round(cap_rate*100, 2))
    
    result_df = pd.DataFrame.from_dict(results)
    result_df.to_csv(output_filename, index=False)