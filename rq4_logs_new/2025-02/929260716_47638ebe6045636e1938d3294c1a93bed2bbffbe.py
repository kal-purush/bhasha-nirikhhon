from flask import Blueprint, request, jsonify
import boto3
import json
import uuid
from flask_cors import cross_origin

wf_s3_bp = Blueprint("wf_s3", __name__)

# AWS S3 Configuration
S3_BUCKET_NAME = "hax-all-data-test"
s3_client = boto3.client("s3")

@wf_s3_bp.route("/upload_workflows", methods=["POST"])
def upload_s3():
    try:
        data = request.json  # Get JSON data from frontend
        user_id = data.get("user_id")  # Unique Google ID
        workflow = data.get("workflow")  # Workflow data
        if not user_id or not workflow:
            return jsonify({"error": "Missing user_id or workflow"}), 400
        
        if isinstance(workflow, str):
            print("Warning: workflow is already a string. Avoiding double encoding.", flush=True)
            workflow_json = workflow  # Keep as is
        else:
            workflow_json = json.dumps(workflow)  # Convert if necessary

        # Generate unique filename
        unique_id = uuid.uuid4().hex
        s3_key = f"{user_id}/workflow_{unique_id}.json"

        # Upload to S3
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=workflow_json)

        return jsonify({"message": "Workflow saved successfully", "s3_path": s3_key})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@wf_s3_bp.route("/get_workflow", methods=["GET"])
def get_workflow():
    user_id = request.args.get("user_id")  # Retrieve from query params
    project_name = request.args.get("project_name")  # Retrieve from query params

    if not user_id or not project_name:
        return jsonify({"error": "Missing user_id or project_name"}), 400

    file_key = f"{user_id}/{project_name}"

    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        workflow_data = json.loads(response["Body"].read().decode("utf-8"))
        return jsonify({"workflow": workflow_data}), 200
    except s3_client.exceptions.NoSuchKey:
        return jsonify({"error": "Workflow not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@wf_s3_bp.route("/list_workflows", methods=["GET"])
def list_workflows():
    user_id = request.args.get("user_id")  # Get user ID from frontend

    if not user_id:
        return jsonify({"error": "Missing user_id"}), 400

    try:
        # Get all objects under the user's directory in S3
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=f"{user_id}/")
        
        if "Contents" not in response:
            return jsonify({"workflows": []})  # No files found

        # Extract workflow file names from S3 response
        workflow_files = [obj["Key"].split("/")[-1] for obj in response["Contents"]]

        # Sort by last modified time (if available) - Optional
        sorted_workflows = sorted(
            response["Contents"], key=lambda x: x["LastModified"], reverse=True
        )
        workflow_names = [obj["Key"].split("/")[-1] for obj in sorted_workflows]

        return jsonify({"workflows": workflow_names})

    except Exception as e:
        return jsonify({"error": str(e)}), 500