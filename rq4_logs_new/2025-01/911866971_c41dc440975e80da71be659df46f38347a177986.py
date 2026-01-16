from flask import Flask, render_template, request, redirect, url_for, render_template_string
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_mail import Mail, Message
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from fpdf import FPDF
from html import unescape 
from xhtml2pdf import pisa


app = Flask(__name__)

# Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///students.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['UPLOAD_FOLDER'] = os.path.join(app.root_path, 'static/uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # Limit upload size to 16MB

db = SQLAlchemy(app)
migrate = Migrate(app, db)

# Mail configuration
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = 'thiganthworkspace@gmail.com'
app.config['MAIL_PASSWORD'] = 'yppt ejfo nocu egkk'
app.config['MAIL_DEFAULT_SENDER'] = 'kthiganth@gmail.com'

mail = Mail(app)

# Ensure the upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Database Model
class Student(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    reg_no = db.Column(db.String(20), nullable=False)
    gender = db.Column(db.String(10), nullable=False)
    start_date = db.Column(db.String(10), nullable=True)  # Change to nullable
    end_date = db.Column(db.String(10), nullable=True)
    reason = db.Column(db.String(200), nullable=False)
    department = db.Column(db.String(100), nullable=False)
    signature_filename = db.Column(db.String(200), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_approved = db.Column(db.Boolean, default=None)
    is_disapproved = db.Column(db.Boolean, default=None)
    email = db.Column(db.String(120), nullable=True)
    attachment_filename = db.Column(db.String(200), nullable=True)
    approved_at = db.Column(db.DateTime, nullable=True)
    hours = db.Column(db.String(200), nullable=True)

    def __repr__(self):
        return f'<Student {self.name}>'

@app.route('/')
def welcome():
    return render_template('welcome.html')

@app.route('/section')
def index():
    return render_template('section_1.html')

@app.route('/confirm', methods=['POST'])
def confirm():
    name = request.form['name']
    reg_no = request.form['reg_no']
    gender = request.form['gender']
    start_date = request.form['start_date']
    end_date = request.form['end_date']
    reason = request.form['reason']
    department = request.form['department']
    signature = request.files['signature'].filename
    attachment = request.files['attachment'].filename if 'attachment' in request.files else None

    return render_template('confirm.html', 
        name=name, 
        reg_no=reg_no, 
        gender=gender, 
        start_date=start_date, 
        end_date=end_date, 
        reason=reason, 
        department=department, 
        signature=signature,
        attachment=attachment
    )


def send_on_duty_approval_email(user_email, approval_status, form_path):
    subject = "On-Duty Form Approval"
    body = f"Your On-Duty form has been {approval_status}. Please find the attached document."

    msg = Message(subject, recipients=[user_email])
    msg.body = body

    # Attach the approved absentee form
    with app.open_resource(form_path) as fp:
        msg.attach("approved_on_duty_form.pdf", "application/pdf", fp.read())  # Change the filename and content type if needed

    mail.send(msg)

def send_approval_email(user_email, approval_status, form_path):
    subject = "Absentee Form Approval"
    body = f"Your absentee form has been {approval_status}. Please find the attached document."

    msg = Message(subject, recipients=[user_email])
    msg.body = body

    # Attach the approved absentee form
    with app.open_resource(form_path) as fp:
        msg.attach("approved_absentee_form.pdf", "application/pdf", fp.read())  # Change the filename and content type if needed

    mail.send(msg)

def generate_pdf_from_html(html_content, file_path):
    # Convert HTML content into a PDF using pisa
    with open(file_path, "w+b") as result_file:
        pisa_status = pisa.CreatePDF(
            src=html_content,           # the HTML content
            dest=result_file            # the file to write the PDF to
        )
        if pisa_status.err:
            return False
    return True



@app.route('/section-grid')
def section_grid():
    return render_template('section_2.html')

@app.route('/absentee-form-1')
def absentee_form_1():
    return render_template('absentee_form_1.html')

@app.route('/submit-1', methods=['POST'])
def submit_1():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    start_date = request.form['start_date']
    end_date = request.form['end_date']
    if not end_date:
            end_date = start_date

    student = Student(
            name=request.form['name'],
            reg_no=request.form['reg_no'],
            gender=request.form['gender'],
            start_date=start_date,
            end_date=end_date,
            reason=request.form['reason'],
            department=request.form['department'],
            signature_filename=signature_filename,
            email=request.form['email'],
            attachment_filename=attachment_filename
        )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
            f"Leave Application from {student.name}",
            recipients=[counsellor_email]
        )
    msg.html = f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a leave form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">From Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.start_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">To Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.end_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('approve_form_1', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('full_day_leave_disagree', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """
    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
            msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())
    mail.send(msg)

    return redirect(url_for('full_day_leave_preview', reg_no=student.reg_no))

@app.route('/full-day-leave-preview/<reg_no>')
def full_day_leave_preview(reg_no):
    student = Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('preview.html', student=student)

@app.route('/approve/<form_id>', methods=['GET', 'POST'])
def approve_form_1(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at = datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    approval_html = render_template('approved_form_1.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(approval_html, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_approval_email(user_email, "approved", form_path)

    return redirect(url_for('full_day_leave_preview', reg_no=form.reg_no))

@app.route('/full-day-leave-disagree/<int:student_id>')
def full_day_leave_disagree(student_id):
    student = Student.query.get_or_404(student_id)
    student.is_approved = False
    student.is_disapproved = True
    #student.disapproved_at= datetime.utcnow()
    db.session.commit()
    
    # Send disapproval email
    msg = Message("Leave Request Disapproved", recipients=[student.email])
    msg.body = f"Your leave request has been disapproved by the Class Counsellor."
    mail.send(msg)
    
    return redirect(url_for('full_day_leave_preview', reg_no=student.reg_no))

@app.route('/absentee-form-2')
def absentee_form_2():
    return render_template('absentee_form_2.html')

@app.route('/submit-2', methods=['POST'])
def submit_2():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        if attachment_file.mimetype not in ['image/png', 'image/jpeg', 'application/pdf']:
            return "Invalid file type", 400
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    selected_hours = request.form.getlist('hours')
    start_date = request.form.get('start_date') if not selected_hours else None
    end_date = request.form.get('end_date') if not selected_hours else None

    hours_str = ', '.join(selected_hours) if selected_hours else None

    student = Student(
        name=request.form['name'],
        reg_no=request.form['reg_no'],
        gender=request.form['gender'],
        start_date=start_date,
        end_date=end_date,
        reason=request.form['reason'],
        department=request.form['department'],
        signature_filename=signature_filename,
        email=request.form['email'],
        attachment_filename=attachment_filename,
        hours=hours_str
    )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
        f"Leave Application from {student.name}",
        recipients=[counsellor_email]
    )
    msg.html =  f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a leave form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">On hours</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.hours}</td>
                </tr>
                
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('approve_form_2', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('specific_hour_leave_disagree', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """

    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
        msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())

    mail.send(msg)

    return redirect(url_for('preview_for_specific_hour', reg_no=student.reg_no))

@app.route('/preview-specific-hour/<reg_no>')
def preview_for_specific_hour(reg_no):
    student = Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('preview_for_specific_hour.html', student=student)

@app.route('/approve_2/<form_id>', methods=['GET', 'POST'])
def approve_form_2(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at = datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    approval_html = render_template('approved_form_2.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(approval_html, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_approval_email(user_email, "approved", form_path)

    return redirect(url_for('preview_for_specific_hour', reg_no=form.reg_no))

@app.route('/specific-hour-leave-disagree/<int:student_id>')
def specific_hour_leave_disagree(student_id):
    student = Student.query.get_or_404(student_id)
    student.is_approved = False
    student.is_disapproved = True
    #student.disapproved_at= datetime.utcnow()
    db.session.commit()
    
    # Send disapproval email
    msg = Message("Leave Request Disapproved", recipients=[student.email])
    msg.body = f"Your leave request has been disapproved by the Class Counsellor."
    mail.send(msg)
    
    return redirect(url_for('preview_for_specific_hour', reg_no=student.reg_no))

"""On-Duty_magics...."""
@app.route('/on-duty-section')
def on_duty_form():
    return render_template('on_duty_section.html')

@app.route('/on-duty-section-grid')
def on_duty_section_grid():
    return render_template('section_3.html')

@app.route('/technical-on-duty-form')
def technical_on_duty_form_full_day():
    return render_template('technical_on_duty_form_full_day.html')

@app.route('/technical-full-day-on-duty-submit', methods=['POST'])
def technical_on_duty_submit_1():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    start_date = request.form['start_date']
    end_date = request.form['end_date']
    if not end_date:
            end_date = start_date

    student = Student(
            name=request.form['name'],
            reg_no=request.form['reg_no'],
            gender=request.form['gender'],
            start_date=start_date,
            end_date=end_date,
            reason=request.form['reason'],
            department=request.form['department'],
            signature_filename=signature_filename,
            email=request.form['email'],
            attachment_filename=attachment_filename
        )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
            f"On-Duty Application from {student.name}",
            recipients=[counsellor_email]
        )
    msg.html = f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a on-duty form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">From Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.start_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">To Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.end_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('technical_on_duty_approve_form_1', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('technical_on_duty_disagree_1', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """
    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
            msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())
    mail.send(msg)

    return redirect(url_for('technical_on_duty_preview_1', reg_no=student.reg_no))

@app.route('/on-duty-preview/<reg_no>')
def technical_on_duty_preview_1(reg_no):
    student= Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('technical_on_duty_preview_1.html',student=student)

@app.route('/on_duty_approve/<form_id>', methods=['GET', 'POST'])
def technical_on_duty_approve_form_1(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at= datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    on_duty_approval_html = render_template('technical_on_duty_approved_form_1.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(on_duty_approval_html, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_on_duty_approval_email(user_email, "approved", form_path)

    return redirect(url_for('technical_on_duty_preview_1', reg_no=form.reg_no))

@app.route('/full-day-on-duty-disagree/<int:student_id>')
def technical_on_duty_disagree_1(student_id):
    student = Student.query.get_or_404(student_id)
    student.is_approved = False
    student.is_disapproved = True
    #student.disapproved_at= datetime.utcnow()
    db.session.commit()
    
    # Send disapproval email
    msg = Message("On-Duty Request Disapproved", recipients=[student.email])
    msg.body = f"Your On-Duty request has been disapproved by the Class Counsellor."
    mail.send(msg)
    
    return redirect(url_for('technical_on_duty_preview_1', reg_no=student.reg_no))

@app.route('/specific-hour-technical-on-duty-form')
def specific_technical_on_duty_form():
    return render_template('specific_technical_on_duty_form.html')

@app.route('/technical-specific-hour-on-duty-submit', methods=['POST'])
def technical_on_duty_submit_2():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        if attachment_file.mimetype not in ['image/png', 'image/jpeg', 'application/pdf']:
            return "Invalid file type", 400
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    selected_hours = request.form.getlist('hours')
    start_date = request.form.get('start_date') if not selected_hours else None
    end_date = request.form.get('end_date') if not selected_hours else None

    hours_str = ', '.join(selected_hours) if selected_hours else None

    student = Student(
        name=request.form['name'],
        reg_no=request.form['reg_no'],
        gender=request.form['gender'],
        start_date=start_date,
        end_date=end_date,
        reason=request.form['reason'],
        department=request.form['department'],
        signature_filename=signature_filename,
        email=request.form['email'],
        attachment_filename=attachment_filename,
        hours=hours_str
    )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
        f"Technical On-Duty Application from {student.name}",
        recipients=[counsellor_email]
    )
    msg.html =  f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a technical on-duty form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">On hours</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.hours}</td>
                </tr>
                
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('technical_on_duty_approve_form_2', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('technical_on_duty_disagree_2', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """

    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
        msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())

    mail.send(msg)

    return redirect(url_for('technical_on_duty_preview_2', reg_no=student.reg_no))

@app.route('/specific-on_duty_approve/<form_id>', methods=['GET', 'POST'])
def technical_on_duty_approve_form_2(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at= datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    on_duty_approval_html = render_template('technical_on_duty_approved_form_2.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(on_duty_approval_html, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_on_duty_approval_email(user_email, "approved", form_path)

    return redirect(url_for('technical_on_duty_preview_2', reg_no=form.reg_no))

@app.route('/technical-on-duty-specific-hour-preview/<reg_no>')
def technical_on_duty_preview_2(reg_no):
    student= Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('technical_on_duty_preview_2.html',student=student)

@app.route('/specific-hour-on-duty-disagree/<int:student_id>')
def technical_on_duty_disagree_2(student_id):
    student = Student.query.get_or_404(student_id)
    student.is_approved = False
    student.is_disapproved = True
    #student.disapproved_at= datetime.utcnow()
    db.session.commit()
    
    # Send disapproval email
    msg = Message("On-duty Request Disapproved", recipients=[student.email])
    msg.body = f"Your On-duty request  for specific hour has been disapproved by the Class Counsellor."
    mail.send(msg)
    
    return redirect(url_for('technical_on_duty_preview_2', reg_no=student.reg_no))

# Non-technical On-Duty modules...

@app.route('/non-technical-on-duty-form-full-day')
def non_technical_on_duty_form_full_day():
    return render_template('non_technical_on_duty_form_full_day.html')

@app.route('/non-technical-on-duty-submit', methods=['POST'])
def non_technical_on_duty_submit_1():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    start_date = request.form['start_date']
    end_date = request.form['end_date']
    if not end_date:
            end_date = start_date

    student = Student(
            name=request.form['name'],
            reg_no=request.form['reg_no'],
            gender=request.form['gender'],
            start_date=start_date,
            end_date=end_date,
            reason=request.form['reason'],
            department=request.form['department'],
            signature_filename=signature_filename,
            email=request.form['email'],
            attachment_filename=attachment_filename
        )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
            f"On-Duty Application from {student.name}",
            recipients=[counsellor_email]
        )
    msg.html = f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a on-duty form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">From Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.start_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">To Date</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.end_date}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('non_technical_on_duty_approve_form_1', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('non_technical_on_duty_disagree_1', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """
    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
            msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())
    mail.send(msg)

    return redirect(url_for('non_technical_on_duty_preview_1', reg_no=student.reg_no))

@app.route('/onon-technical-On-Duty-approve/<form_id>', methods=['GET', 'POST'])
def non_technical_on_duty_approve_form_1(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at= datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    on_duty_approval_html = render_template('technical_on_duty_approved_form_1.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(on_duty_approval_html, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_on_duty_approval_email(user_email, "approved", form_path)

    return redirect(url_for('non_technical_on_duty_preview_1', reg_no=form.reg_no))

@app.route('/non-technical-full-day-on-duty-disagree/<int:student_id>')
def non_technical_on_duty_disagree_1(student_id):
    student = Student.query.get_or_404(student_id)
    student.is_approved = False
    student.is_disapproved = True
    #student.disapproved_at= datetime.utcnow()
    db.session.commit()
    
    # Send disapproval email
    msg = Message("On-Duty Request Disapproved", recipients=[student.email])
    msg.body = f"Your On-Duty request has been disapproved by the Class Counsellor."
    mail.send(msg)
    
    return redirect(url_for('non_technical_on_duty_preview_1', reg_no=student.reg_no))

@app.route('/non-technical-on-duty-preview/<reg_no>')
def non_technical_on_duty_preview_1(reg_no):
    student= Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('technical_on_duty_preview_1.html',student=student)

@app.route('/specific-hour-non-technical-on-duty-form')
def specific_non_technical_on_duty_form():
    return render_template('specific_non_technical_on_duty_form.html')

@app.route('/non-technical-on-duty-form')
def non_technical_on_duty_form():
    return render_template('section_4.html')

@app.route('/non-technical-specific-hour-on-duty-submit', methods=['POST'])
def non_technical_on_duty_submit_2():
    if 'signature' not in request.files:
        return "No file part", 400
    
    signature_file = request.files['signature']
    attachment_file = request.files['attachment']
    
    if signature_file.filename == '':
        return "No selected file", 400

    if signature_file:
        signature_filename = secure_filename(signature_file.filename)
        signature_file.save(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename))

    attachment_filename = None
    if attachment_file and attachment_file.filename != '':
        if attachment_file.mimetype not in ['image/png', 'image/jpeg', 'application/pdf']:
            return "Invalid file type", 400
        attachment_filename = secure_filename(attachment_file.filename)
        attachment_file.save(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename))

    selected_hours = request.form.getlist('hours')
    start_date = request.form.get('start_date') if not selected_hours else None
    end_date = request.form.get('end_date') if not selected_hours else None

    hours_str = ', '.join(selected_hours) if selected_hours else None

    student = Student(
        name=request.form['name'],
        reg_no=request.form['reg_no'],
        gender=request.form['gender'],
        start_date=start_date,
        end_date=end_date,
        reason=request.form['reason'],
        department=request.form['department'],
        signature_filename=signature_filename,
        email=request.form['email'],
        attachment_filename=attachment_filename,
        hours=hours_str
    )

    db.session.add(student)
    db.session.commit()

    counsellor_email = 'kthiganth@gmail.com'
    msg = Message(
        f"Non-Technical On-Duty Application from {student.name}",
        recipients=[counsellor_email]
    )
    msg.html =  f"""
            <p>Dear Counsellor,</p>
            <p>The student {student.name} (Reg No: {student.reg_no}) has submitted a Non-Technical On-duty form. Below are the details:</p>
            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
                <tr style="background-color: #f4f4f4;">
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Field</th>
                    <th style="padding: 10px; border: 1px solid #ddd; text-align: left;">Value</th>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Name</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.name}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Registration Number</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reg_no}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Gender</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.gender}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">On hours</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.hours}</td>
                </tr>
                
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Reason</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.reason}</td>
                </tr>
                <tr>
                    <td style="padding: 10px; border: 1px solid #ddd;">Department</td>
                    <td style="padding: 10px; border: 1px solid #ddd;">{student.department}</td>
                </tr>
            </table>
            <p>Please review the request and take appropriate action:</p>
            <a href="{url_for('non_technical_on_duty_approve_form_2', form_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: #28a745; color: white; text-decoration: none; border-radius: 5px;">Agree</a>
           
            <a href="{url_for('technical_on_duty_disagree_2', student_id=student.id, _external=True)}" style="padding: 10px 20px; background-color: red; color: white; text-decoration: none; border-radius: 5px;">Disagree</a>
            <p>Best regards,<br>The Attendance System</p>
        """

    with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], signature_filename)) as fp:
        msg.attach(signature_filename, "image/png", fp.read())
    
    if attachment_filename:
        with app.open_resource(os.path.join(app.config['UPLOAD_FOLDER'], attachment_filename)) as fp:
            msg.attach(attachment_filename, "image/png", fp.read())

    mail.send(msg)

    return redirect(url_for('non_technical_on_duty_preview_2', reg_no=student.reg_no))

@app.route('/non-technical-on-duty-specific-hour-preview/<reg_no>')
def non_technical_on_duty_preview_2(reg_no):
    student= Student.query.filter_by(reg_no=reg_no).order_by(Student.created_at.desc()).first_or_404()
    return render_template('technical_on_duty_preview_2.html',student=student)

@app.route('/specific-non-technical-on-duty-approve/<form_id>', methods=['GET', 'POST'])
def non_technical_on_duty_approve_form_2(form_id):
    # Fetch the form and update its status
    form = Student.query.get_or_404(form_id)
    form.is_approved = True
    form.approved_at= datetime.utcnow()
    db.session.commit()  # Commit changes

    # Generate the approval HTML content
    on_duty_approval_html_1 = render_template('non_technical_on_duty_approve_form_2.html', student=form)

    # Save the PDF to a file
    form_path = f"static/approved_forms/{form_id}_approved.pdf"
    if generate_pdf_from_html(on_duty_approval_html_1, form_path):
        # Send the PDF via email
        user_email = form.email  # Assuming you have the student's email
        send_on_duty_approval_email(user_email, "approved", form_path)

    return redirect(url_for('non_technical_on_duty_preview_2', reg_no=form.reg_no))
if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create the database tables
    app.run(debug=True)