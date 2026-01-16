from flask import jsonify, Blueprint
import os
from supabase import create_client, Client
from dotenv import load_dotenv


load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(url, key)


supabase_blueprint: Blueprint = Blueprint('supabase', __name__)


@supabase_blueprint.route(rule="/test_supabase", methods=["GET"])
def test_supabase():
    try:
        data, _ = supabase.table('test').select("*").execute()
        return jsonify(data[1])
    except Exception as e:
        return jsonify({
            'error': str(e)
        })