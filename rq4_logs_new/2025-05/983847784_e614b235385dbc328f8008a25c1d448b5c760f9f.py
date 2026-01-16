import os
import logging
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Setup database base class
class Base(DeclarativeBase):
    pass

# Initialize SQLAlchemy
db = SQLAlchemy(model_class=Base)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Database configuration - using SQLite
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///aitesttool.db"
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Configure file uploads
app.config["UPLOAD_FOLDER"] = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
app.config["ALLOWED_EXTENSIONS"] = {"txt", "py", "js", "java", "cs", "cpp", "c", "php", "rb", "go", "ts", 
                             "html", "css", "json", "xml", "md"}
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16MB max upload

# Create upload directory if it doesn't exist
os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# Initialize the database
db.init_app(app)

# Import models after db initialization
from models import Project, TestRun, TestCase

# Create database tables
with app.app_context():
    db.create_all()

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config["ALLOWED_EXTENSIONS"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    # Get most recent projects and test runs for the dashboard
    projects = Project.query.order_by(Project.created_at.desc()).limit(5).all()
    runs = TestRun.query.order_by(TestRun.created_at.desc()).limit(10).all()
    
    # Prepare stats and chart data
    stats = {
        'total_projects': Project.query.count(),
        'total_runs': TestRun.query.count(),
        'completed_runs': TestRun.query.filter_by(status='completed').count(),
        'failed_runs': TestRun.query.filter_by(status='failed').count()
    }
    
    # Activity data (test runs per day for the last 7 days)
    from sqlalchemy import func
    
    # Get date labels for the last 7 days
    today = datetime.now()
    date_labels = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(6, -1, -1)]
    
    # Count runs for each day
    activity_values = []
    for date_str in date_labels:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        next_day = date_obj + timedelta(days=1)
        count = TestRun.query.filter(
            TestRun.created_at >= date_obj,
            TestRun.created_at < next_day
        ).count()
        activity_values.append(count)
    
    chart_data = {
        'activity_labels': date_labels,
        'activity_values': activity_values
    }
    
    return render_template('dashboard.html', projects=projects, runs=runs, stats=stats, chart_data=chart_data)

@app.route('/generate', methods=['GET'])
def generate():
    return render_template('test_generation.html')

@app.route('/test-generation', methods=['POST'])
def test_generation():
    try:
        project_name = request.form.get('project_name')
        input_type = request.form.get('input_type')
        input_content = request.form.get('input_content', '')
        test_type = request.form.get('test_type')
        
        # Check if file was uploaded
        input_file = request.files.get('input_file')
        
        if input_file and input_file.filename:
            filename = secure_filename(input_file.filename)
            if allowed_file(input_file.filename):
                content = input_file.read().decode('utf-8', errors='ignore')
                input_content = content
            else:
                flash("Unsupported file type", "danger")
                return redirect(url_for('generate'))
        
        # Validate input
        if not project_name or not input_content.strip():
            flash("Project name and input content are required", "danger")
            return redirect(url_for('generate'))
        
        # Create project
        project = Project(
            name=project_name,
            description=f"{test_type.title()} test generation based on {input_type}"
        )
        db.session.add(project)
        db.session.flush()  # Get the project ID
        
        # Create test run
        test_run = TestRun(
            project_id=project.id,
            test_type=test_type,
            input_type=input_type,
            input_content=input_content,
            status="processing"
        )
        db.session.add(test_run)
        db.session.commit()
        
        # Process the input and generate test cases
        from utils import generate_test_cases
        
        # Generate test cases based on the input
        test_cases = generate_test_cases(input_content, test_type, input_type)
        
        # Save test cases to the database
        for i, tc in enumerate(test_cases):
            test_case = TestCase(
                test_run_id=test_run.id,
                test_id=f"TC-{i+1}",  # Format test case IDs as TC-1, TC-2, etc.
                name=tc['name'],
                description=tc['description'],
                preconditions=tc.get('preconditions', ''),
                expected_result=tc.get('expected_result', ''),
                steps=tc.get('steps', [])
            )
            db.session.add(test_case)
        
        # Update test run status
        test_run.status = "completed"
        test_run.completed_at = datetime.now()
        db.session.commit()
        
        flash("Test cases generated successfully", "success")
        return redirect(url_for('results', run_id=test_run.id))
        
    except Exception as e:
        logger.exception("Error generating test cases")
        flash(f"An error occurred: {str(e)}", "danger")
        return redirect(url_for('generate'))

@app.route('/history')
def history():
    projects = Project.query.order_by(Project.created_at.desc()).all()
    
    # Load test runs for each project
    for project in projects:
        project.test_runs = TestRun.query.filter_by(project_id=project.id).order_by(TestRun.created_at.desc()).all()
    
    return render_template('history.html', projects=projects)

@app.route('/results/<int:run_id>')
def results(run_id):
    test_run = TestRun.query.get_or_404(run_id)
    project = Project.query.get_or_404(test_run.project_id)
    test_cases = TestCase.query.filter_by(test_run_id=run_id).all()
    
    # Transform test cases for template
    results = []
    for tc in test_cases:
        results.append({
            'id': tc.id,
            'test_id': tc.test_id,
            'name': tc.name,
            'description': tc.description,
            'preconditions': tc.preconditions,
            'expected_result': tc.expected_result,
            'steps': tc.steps
        })
    
    return render_template('results.html', test_run=test_run, project=project, results=results)

@app.route('/export/<int:run_id>/<format>')
def export(run_id, format):
    test_run = TestRun.query.get_or_404(run_id)
    test_cases = TestCase.query.filter_by(test_run_id=run_id).all()
    
    if format == 'json':
        # Export as JSON
        results = []
        for tc in test_cases:
            results.append({
                'test_id': tc.test_id,
                'name': tc.name,
                'description': tc.description,
                'preconditions': tc.preconditions,
                'expected_result': tc.expected_result,
                'steps': tc.steps
            })
        return jsonify(results)
    
    elif format == 'csv':
        # Export as CSV
        import csv
        from io import StringIO
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Test ID', 'Name', 'Description', 'Preconditions', 'Expected Result', 'Steps'])
        
        # Write data
        for tc in test_cases:
            writer.writerow([
                tc.test_id,
                tc.name,
                tc.description,
                tc.preconditions,
                tc.expected_result,
                ', '.join(tc.steps) if isinstance(tc.steps, list) else tc.steps
            ])
        
        # Return CSV as response
        from flask import Response
        response = Response(
            output.getvalue(),
            mimetype='text/csv',
            content_type='text/csv',
        )
        response.headers['Content-Disposition'] = f'attachment; filename=test_cases_{run_id}.csv'
        return response
    
    else:
        flash("Unsupported export format", "danger")
        return redirect(url_for('results', run_id=run_id))

@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
def delete_project(project_id):
    try:
        project = Project.query.get_or_404(project_id)
        
        # Delete associated test runs and test cases
        test_runs = TestRun.query.filter_by(project_id=project_id).all()
        for run in test_runs:
            TestCase.query.filter_by(test_run_id=run.id).delete()
            db.session.delete(run)
        
        db.session.delete(project)
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        logger.exception("Error deleting project")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/runs/<int:run_id>', methods=['DELETE'])
def delete_run(run_id):
    try:
        test_run = TestRun.query.get_or_404(run_id)
        
        # Delete associated test cases
        TestCase.query.filter_by(test_run_id=run_id).delete()
        
        db.session.delete(test_run)
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        logger.exception("Error deleting test run")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/testcases/<int:testcase_id>', methods=['DELETE'])
def delete_testcase(testcase_id):
    try:
        test_case = TestCase.query.get_or_404(testcase_id)
        
        db.session.delete(test_case)
        db.session.commit()
        
        return jsonify({'success': True})
    except Exception as e:
        db.session.rollback()
        logger.exception("Error deleting test case")
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5200, debug=True)