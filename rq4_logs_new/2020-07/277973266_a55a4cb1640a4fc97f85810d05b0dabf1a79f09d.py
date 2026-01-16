def addRevenue(amount, server, person, cust):
    if cust:
        store = server.sqlSelect("SELECT customer_storePreference FROM customers WHERE customer_phone = %s ;"%person)
        store = store.fetchone()
        totrev = server.sqlSelect("SELECT store_sales FROM store WHERE store_name = '%s';"%store) 
        totrev = totrev.fetchone()
        flrev = float(totrev[0])
        flrev = flrev + amount
        store = server.sqlInsert("UPDATE store SET store_sales = %.2f WHERE store_name = %s;" %(flrev, store))