wsize=200
wsize=str(wsize)
hsize=200
hsize=str(hsize)

img_sub_dict={'{@width}':wsize,'{@height}':hsize,'?q={@quality}':''}
def fprod_title(items):
    try:
        prod_title = items['productInfo']['value']['titles']['title']
        #print prod_Title
    except:
         prod_title = 'No Title Info'
    return prod_title


def fprod_stock(items):
    try:
        prod_stock = items['productInfo']['value']['availability']['displayState']
        #print prod_stock
    except:
        prod_stock = 'No Stock Info'
    return prod_stock


def frtng_avg(items):
    try:
        rtng_avg = items['productInfo']['value']['rating']['average']
        #print rtng_avg
    except:
        rtng_avg = 'No Rating_Avg Info'
    return rtng_avg

def fprod_price(items):
    try:
        prod_price= items['productInfo']['value']['pricing']['finalPrice']['decimalValue']
        #print rtng_avg
    except:
        prod_price = 'No Prod Price Info'
    return prod_price


def fdelvry_price(items):
    try:
        delvry_price = items['productInfo']['value']['pricing']['deliveryCharge']['decimalValue']
        #print delvry_price
    except:
        delvry_price = 'No Delvry Price Info'
    return delvry_price


def fprod_url(items):
    try:
        prod_url = 'https://www.flipkart.com'+items['productInfo']['value']['baseUrl']
        #print prod_url
    except:
        prod_url = 'No Url Info'
    return prod_url

def ffinal_price(items):
    pp = fprod_price(items)
    dp = fdelvry_price(items)
    try:
        final_price = pp + dp
        #print delvry_price
    except:
        final_price = 'No Final Price Info'
    return final_price

def fprod_img(items):
    img_list = []
    try:
        img_iter = items['productInfo']['value']['media']['images']
        for img in img_iter:
            # printimg['url']
            img_list.append(reduce(lambda a, kv: a.replace(*kv), img_sub_dict.iteritems(), img['url']))
        #print img_list
    except:
        img_list = 'No Images are retrieved'
    return img_list[0]

def fprod_warnty(items):
    try:
        prod_warnty= items['productInfo']['value']['warrantySummary']
        #print prod_warnty
    except:
        prod_warnty = 'No Prod Warranty Info'
    return prod_warnty