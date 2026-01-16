from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import requests
import logging
from datetime import datetime
import os
from dotenv import load_dotenv
from flask_cors import CORS

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)
CORS(app)
# Initialize API key from environment variable
API_KEY = os.getenv('HELIUS_API_KEY', '4b72d62d-6176-4939-918b-b486fe647122')
if not API_KEY:
    raise ValueError("HELIUS_API_KEY environment variable is not set")

class RugPullDetector:
    def __init__(self, api_key):
        self.API_KEY = api_key
        self.RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={self.API_KEY}"
        self.session = requests.Session()

    def get_token_metadata(self, token_address: str):
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getAsset",
            "params": [token_address]
        }

        headers = {
            "Content-Type": "application/json"
        }

        try:
            response = self.session.post(self.RPC_URL, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if "result" in data:
                return data["result"]
            return None
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching token metadata: {str(e)}")
            return None

    def get_token_supply(self, token_address: str):
        payload = {
            "jsonrpc": "2.0",
            "id": "my-id",
            "method": "getTokenSupply",
            "params": [token_address]
        }

        headers = {
            "Content-Type": "application/json"
        }

        try:
            response = self.session.post(self.RPC_URL, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            if "result" in data and "value" in data["result"]:
                return data["result"]["value"]
            return None
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching token supply: {str(e)}")
            return None

    def calculate_risk_score(self, token_data, supply_data):
        risk_score = 0.0
        risk_factors = []
        
        if not token_data:
            return 1.0, ["No token metadata available"]
        
        if not token_data.get("name"):
            risk_score += 0.2
            risk_factors.append("Missing token name")
            
        if not token_data.get("symbol"):
            risk_score += 0.1
            risk_factors.append("Missing token symbol")

        if supply_data:
            try:
                supply = float(supply_data.get("uiAmount", 0))
                if supply == 0:
                    risk_score += 0.3
                    risk_factors.append("Zero supply")
                elif supply > 1000000000000:
                    risk_score += 0.2
                    risk_factors.append("Extremely high supply")
            except:
                risk_score += 0.2
                risk_factors.append("Invalid supply data")

        decimals = supply_data.get("decimals") if supply_data else None
        if decimals is None or decimals == 0:
            risk_score += 0.2
            risk_factors.append("Invalid decimals")

        if token_data.get("frozen"):
            risk_score += 0.3
            risk_factors.append("Token is frozen")

        return min(risk_score, 1.0), risk_factors

    def analyze_token(self, token_address: str):
        token_data = self.get_token_metadata(token_address)
        supply_data = self.get_token_supply(token_address)
        
        if not token_data and not supply_data:
            return {
                "error": "Unable to fetch token data",
                "status": "error"
            }
        
        risk_score, risk_factors = self.calculate_risk_score(token_data, supply_data)
        
        return {
            "status": "success",
            "data": {
                "token_address": token_address,
                "name": token_data.get("name", "Unknown") if token_data else "Unknown",
                "symbol": token_data.get("symbol", "Unknown") if token_data else "Unknown",
                "supply": supply_data.get("uiAmount", 0) if supply_data else 0,
                "decimals": supply_data.get("decimals", 0) if supply_data else 0,
                "risk_score": risk_score,
                "risk_factors": risk_factors,
                "risk_level": "HIGH" if risk_score > 0.7 else "MEDIUM" if risk_score > 0.4 else "LOW",
                "timestamp": datetime.now().isoformat()
            }
        }

# Create detector instance
detector = RugPullDetector(API_KEY)

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "ok",
        "message": "Rug Pull Detector API",
        "version": "1.0.0",
        "endpoints": {
            "health_check": "/health",
            "analyze_token": "/analyze?token=TOKEN_ADDRESS"
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "environment": os.getenv('ENVIRONMENT', 'production')
    })

@app.route('/analyze', methods=['GET'])
def analyze():
    token_address = request.args.get('token')
    
    if not token_address:
        return jsonify({
            "error": "Token address is required",
            "status": "error"
        }), 400
    
    try:
        result = detector.analyze_token(token_address)
        return jsonify(result)
    except Exception as e:
        logging.error(f"Error analyzing token: {str(e)}")
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    if os.getenv('ENVIRONMENT') == 'development':
        app.run(host='0.0.0.0', port=port, debug=True)
    else:
        # In production, we'll use gunicorn instead of waitress
        app.run(host='0.0.0.0', port=port)
