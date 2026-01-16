import requests
from bs4 import BeautifulSoup
import boto3
import hashlib
import uuid
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ScrapedProducts_amazon')

# URL of the Amazon search results page
# url = 'https://www.amazon.in/s?k=sofa+3+seater&i=furniture&rh=n%3A5689463031%2Cp_90%3A6741118031%2Cp_n_material_two_browse-bin%3A5785127031&dc&ds=v1%3AfwVAsa4XMLro0z19zgrlCeFbJIcAKEa3FdtXtBlzrww&crid=EG31QNQJ2KBV&qid=1726493407&rnid=1480452031&sprefix=sofa+%2Caps%2C256&ref=sr_nr_p_n_material_two_browse-bin_2'
url = 'https://www.amazon.in/Torque-Seater-Fabric-Furniture-Living/dp/B0BPS9QS7R/ref=sr_1_9?crid=1W0JSTTBM67L0&dib=eyJ2IjoiMSJ9.Fm7AXdmtJKsFY-XxeqQvau7qDoqcyGghIJt901Df-Y-1YuMwvpP-dYjutQ2e_lfPAk3bvwg_hw_ctoYpbIkfoqwGCHhpYCxIlnuEs2W7C2elmfPgoS6V4AJPPW4vpKneyxyPYnXylXV_7kITHMSXeFkKaK_CMxSCIkgawewVeC8ieNL-xqhIlXPFpJAGVRLHu52FxWCpdKbGkiJMCAaLb2w482E5axgAAVccNGjyH6bQJRxfsoFEeEjCGLYQ5BrrJhx8I9BDAg0WUTesq8Xoan2F0Ha06qcKx4oQ5Ld11p8.2dh-yVLZjA97e8p4PKX6ca2ofDJXF56Wx7E-632S9MQ&dib_tag=se&keywords=sofa+3+seater&qid=1727806787&sprefix=sofa%2Caps%2C247&sr=8-9'

# Headers to mimic a real browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def generate_product_id(product_url):
    """Generate a unique ProductID using a hash of the product URL or UUID."""
    if product_url != 'No URL':
        return hashlib.sha256(product_url.encode('utf-8')).hexdigest()
    else:
        return str(uuid.uuid4())  # If no URL, generate a UUID


# Function to scrape a category page
def fetch_product_data(url):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Check for HTTP request errors
        soup = BeautifulSoup(response.text, 'html.parser')
        product_data = []
        
        search_conditions = [{'data-component-type': 's-search-result'}, {'id': 'ppd'}]

        for condition in search_conditions:
            for item in soup.find_all('div', condition):
                product = {}
            
                # Extracting product name
                name_tag = item.find('span', {'class': 'a-size-base-plus a-color-base a-text-normal'})
                if not name_tag:
                    name_tag = item.find('span', {'class': 'a-size-large product-title-word-break'})
                if name_tag:
                    product['name'] = name_tag.text
                
                # Extracting product price
                price_tag = item.find('span', {'class': 'a-price-whole'})
                product['price'] = price_tag.text if price_tag else 'No price'
                
                # Extracting product URL
                try:
                    link_tag = item.find('a', {'class': 'a-link-normal s-no-outline'})
                    product_url = 'https://www.amazon.in' + link_tag['href'] 
                except:
                    product_url = url
                product['url'] = product_url
                
                # Extracting product image URL
                img_tag = item.find('img', {'class': 's-image'})
                if not img_tag:
                    img_tag = item.find('img', {'class': 'a-dynamic-image a-stretch-vertical'})
                if img_tag:
                    product['image_url'] = img_tag['src']
                
                # Generating and adding ProductID
                product['ProductID'] = generate_product_id(product_url)
                
                # Store product in list
                product_data.append(product)

                # Print the results for each product
                print("Product Name:", product['name'])
                print("Price:", product['price'])
                print("Image URL:", product['image_url'])
                print("Product URL:", product['url'])
                print('-' * 40)  # Separator for better readability
        
        return product_data
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return []

def store_in_dynamodb(products):
    for product in products:
        try:
            table.put_item(Item=product)
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"AWS credentials error: {e}")
        except Exception as e:
            print(f"Error storing item: {e}")

def main():
    products = fetch_product_data(url)
    if products:
        store_in_dynamodb(products)
    else:
        print("No products found.")

if __name__ == '__main__':
    main()
