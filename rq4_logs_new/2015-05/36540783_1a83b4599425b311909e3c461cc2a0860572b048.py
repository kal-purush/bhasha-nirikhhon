# -*- encoding: utf-8 -*-
##############################################################################
#
#    OpenERP, Open Source Management Solution
#    Copyright (C) 2004-2009 Tiny SPRL (<http://tiny.be>).
#    Copyright (C) 2015 Haiforce.com
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################
from openerp.osv import fields, osv
import time
import openerp
from datetime import datetime
from openerp.tools.translate import _
from openerp.tools import DEFAULT_SERVER_DATE_FORMAT, DEFAULT_SERVER_DATETIME_FORMAT, image_colorize, image_resize_image_big


class Patient_Patient(osv.Model):
    ''' Defining a Patient information '''

    def _calc_age(self, cr, uid, ids, name, arg, context=None):
        ''' This function will automatically calculates the age of particular Patient.'''
        res= {}
        for patient in self.browse(cr, uid, ids, context=context):
            start = datetime.strptime(patient.date_of_birth, DEFAULT_SERVER_DATE_FORMAT)
            end = datetime.strptime(time.strftime(DEFAULT_SERVER_DATE_FORMAT),DEFAULT_SERVER_DATE_FORMAT)
            delta = end - start
            years =  (delta.days / 365)
            res[patient.id] = years
        return res
    
    def create(self, cr, uid, data, context={}):
        if data.get('pid',False):
            data['login']= data['pid']
            data['password']= data['pid']
        else:
            raise osv.except_osv(_('Error!'), _('PID not valid, so record will not save.'))
        result = super(Patient_Patient, self).create(cr, uid, data, context=context)
        return result
    
    _name = 'patient.patient'
   
    _description = 'Patient Information'
    _inherits = {'res.users': 'user_id'}
    _columns = {
        'user_id': fields.many2one('res.users', 'User ID', ondelete="cascade", select=True, required=True),
        'patient_name' : fields.related('user_id', 'name',type='char', size=64, store=True, readonly=True),
        'pid':fields.char('Patient ID', size=64, required=True, help='Personal IDentification Number'),
        # 'reg_code':fields.char('Registration Code', size=64, required=True, help='Patient Registration Code'),
        'Patient_code':fields.char('Patient Code', size=64, required=True),
        'contact_phone1': fields.char('Phone no.',size=20),
        'contact_mobile1': fields.char('Mobile no',size=20),
        # 'roll_no':fields.integer('Roll No.',readonly=True),
        'photo': fields.binary('Photo'),
        # 'year':fields.many2one('academic.year', 'Academic Year', required=True, states={'done':[('readonly',True)]}),
        'cast_id':fields.many2one('patient.cast','Religion'),
        # 'admission_date':fields.date('Admission Date'),
        'middle': fields.char('Middle Name', size=64, required=True, states={'done':[('readonly',True)]}),
        'last': fields.char('Surname', size=64, required=True, states={'done':[('readonly',True)]}),
        'gender':fields.selection([('male','Male'), ('female','Female')], 'Gender', states={'done':[('readonly',True)]}),
        'date_of_birth':fields.date('Birthdate', required=True, states={'done':[('readonly',True)]}),
        # 'mother_tongue':fields.many2one('mother.toungue',"Mother Tongue"),
        'age':fields.function(_calc_age, method=True, string='Age', readonly=True, type="integer"),
        'maritual_status':fields.selection([('unmarried','Unmarried'), ('married','Married')], 'Maritual Status', states={'done':[('readonly',True)]}),
        'reference_ids':fields.one2many('patient.reference', 'reference_id', 'References', states={'done':[('readonly',True)]}),
        # 'previous_hospital_ids':fields.one2many('Patient.previous.hospital', 'previous_hospital_id', 'Previous hospital Detail', states={'done':[('readonly',True)]}),
        'family_con_ids':fields.one2many('patient.family.contact', 'family_contact_id', 'Family Contact Detail', states={'done':[('readonly',True)]}),
        'doctor': fields.char('Doctor Name', size=64, states={'done':[('readonly',True)]} ),
        # 'designation': fields.char('Designation', size=64),
        'doctor_phone': fields.char('Phone', size=12),
        'blood_group': fields.char('Blood Group', size=12),
        'height': fields.float('Height'),
        'weight': fields.float('Weight'),
        'eye':fields.boolean('Eyes'),
        'ear':fields.boolean('Ears'),
        'nose_throat':fields.boolean('Nose & Throat'),
        'respiratory':fields.boolean('Respiratory'),
        'cardiovascular':fields.boolean('Cardiovascular'),
        'neurological':fields.boolean('Neurological'),
        'muskoskeletal':fields.boolean('Musculoskeletal'),
        'dermatological':fields.boolean('Dermatological'),
        'blood_pressure':fields.boolean('Blood Pressure'),
        'remark':fields.text('Remark', states={'done':[('readonly',True)]}),
        # 'hospital_id': fields.many2one('hospital.hospital', 'Hospital', states={'done':[('readonly',True)]}),
        'state':fields.selection([('new','New'),('done','Done'),('terminate','Terminate'),('close','Close')],'State',readonly=True),
        'history_ids': fields.one2many('clinic.history', 'patient_id', 'History'),
     #   'attendance_ids' : fields.one2many('attendance.sheet.line','name','Attendance History',readonly=True),
        'exam_results_ids' : fields.one2many('exam.result','patient_id','Exam History',readonly=True),
    #    'patient_attachment_line' : fields.one2many('patient.attachment','patient_id','Attachment'),
        'address_ids' : fields.one2many('res.partner','patient_id','Contacts'),
        'document':fields.one2many('patient.document','doc_id','Documents'),
        'description':fields.one2many('patient.description','des_id','Description'),
        'patient_id': fields.many2one('patient.patient','name'),
        'contact_phone':fields.related('patient_id','phone',type='char',relation='Patient.Patient',string='Phone No'),
        'contact_mobile':fields.related('patient_id','mobile',type='char',relation='Patient.Patient',string='Mobile No'),
        # 'patient_id': fields.many2one('patient.patient','Name'),
        # 'contact_phone':fields.related('patient_id','phone',type='char',relation='Patient.Patient',string='Phone No',readonly=True),
        # 'contact_mobile':fields.related('patient_id','mobile',type='char',relation='Patient.Patient',string='Mobile No',readonly=True),
        'contact_email':fields.related('patient_id','email',type='char',relation='Patient.Patient',string='Email',readonly=True),
        'contact_website':fields.related('patient_id','website',type='char',relation='Patient.Patient',string='Website',readonly=True),
        'award_list':fields.one2many('Patient.award','award_list_id','Award List'),
        'patient_status':fields.related('state',type='char',relation='Patient.Patient',string='Status',help="Show the Status Of Patient",readonly=True),
        'stu_name':fields.related('user_id','name',type='char',relation='Patient.Patient',string='First Name',readonly=True),
        # 'Acadamic_year':fields.related('year','name',type='char',relation='Patient.Patient',string='Academic Year',help="Academic Year",readonly=True),
        # 'grn_number': fields.many2one('Patient.grn','GR No.',help="General reg number"),
        # 'standard_id':fields.many2one('standard.standard', 'Class'),
        'division_id':fields.many2one('standard.division', 'Division'),
        # 'medium_id':fields.many2one('standard.medium', 'Medium'),
        'hospital_id':fields.related('hospital_id','company_id',relation="res.company", string="Company Name", type="many2one", store=True),
        }
    
    _sql_constraints = [('pid_unique', 'unique(pid_number)', 'GRN Number must be unique!')]

    _defaults = {
        'pid': lambda obj, cr, uid, context:obj.pool.get('ir.sequence').get(cr, uid, 'Patient.Patient'),
        'reg_code': ' ',
        'patient_code':' ',
        'state':'draft',
        # 'admission_date':fields.date.context_today,
        'photo': lambda self, cr, uid, ctx: self._get_default_image(cr, uid, ctx.get('default_is_company', False), ctx),
    }
    
    # def set_to_draft(self, cr, uid, ids, context=None):
    #     self.write(cr, uid, ids, {'state' : 'draft'}, context=context)
    #     return True
    #
    # def set_alumni(self, cr, uid, ids, context=None):
    #     self.write(cr, uid, ids, {'state' : 'alumni'}, context=context)
    #     return True
    #
    # def set_terminate(self, cr, uid, ids, context=None):
    #     self.write(cr, uid, ids, {'state' : 'terminate'}, context=context)
    #     return True
    #
    # def set_done(self, cr, uid, ids, context=None):
    #     self.write(cr, uid, ids, {'state' : 'done'}, context=context)
    #     return True
        
    def _get_default_image(self, cr, uid, is_company, context=None, colorize=False):
        image = image_colorize(open(openerp.modules.get_module_resource('base', 'static/src/img', 'avatar.png')).read())
        return image_resize_image_big(image.encode('base64'))
    
    # def admission_draft(self, cr, uid, ids, context=None):
    #     self.write(cr, uid, ids, {'state' : 'draft'}, context=context)
    #     return True
    #
    # def admission_done(self, cr, uid, ids, context=None):
    #     hospital_standard_obj = self.pool.get('hospital.standard')
    #     for Patient_data in self.browse(cr, uid, ids, context=context):
    #         if Patient_data.age <=5:
    #             raise osv.except_osv(_('Warning'), _('The Patient is not eligible. Age is not valid.'))
    #         domain = [('standard_id', '=', Patient_data.standard_id.id)]
    #         hospital_standard_search_ids = hospital_standard_obj.search(cr, uid, domain, context=context)
    #         if not hospital_standard_search_ids:
    #             raise osv.except_osv(_('Warning'), _('The standard is not defined in a hospital'))
    #         domain = [('standard_id', '=', Patient_data.standard_id.id)]
    #         Patient_search_ids = self.search(cr, uid, domain, context=context)
    #         number = 1
    #         for Patient in self.browse(cr, uid, Patient_search_ids, context=context):
    #             self.write(cr, uid, [Patient.id], {'roll_no':number}, context=context)
    #             number += 1
    #         reg_code = self.pool.get('ir.sequence').get(cr, uid, 'Patient.registration')
    #         registation_code = str(Patient_data.hospital_id.state_id.name) + str('/') + str(Patient_data.hospital_id.city) + str('/') + str(Patient_data.hospital_id.name) + str('/') + str(reg_code)
    #         stu_code = self.pool.get('ir.sequence').get(cr, uid, 'Patient.code')
    #         Patient_code = str(Patient_data.hospital_id.code) + str('/') + str(Patient_data.year.code) + str('/') + str(stu_code)
    #     self.write(cr, uid, ids, {'state': 'done', 'admission_date': time.strftime('%Y-%m-%d'), 'Patient_code' : Patient_code, 'reg_code':registation_code}, context=context)
    #     return True

class attendance_type(osv.Model):
    '''Defining a subject '''
    _name = "attendance.type"
    _description = "clinical Type"
    _columns = {
        'name': fields.char('Name', size=64, required=True),
        'code': fields.char('Code', size=12, required=True),
    }

class Patient_Document(osv.Model):
    _name = 'patient.document'
    _rec_name="doc_type"
    _defaults = {
        'file_no':lambda obj, cr, uid, context:obj.pool.get('ir.sequence').get(cr, uid,'Patient.document'),
    }
    _columns = {
        'doc_id' : fields.many2one('Patient.Patient', 'Patient'),
        'file_no':fields.char('File No', size=12,readonly="1"),
        'submited_date':fields.date('Submitted Date'),
        'doc_type' : fields.many2one('document.type', 'Document Type', required=True),
        'file_name':fields.char('File Name',size=30),
        'return_date':fields.date('Return Date'),
        'new_datas' : fields.binary('Attachments'),
    }

class document_type(osv.Model):
    ''' Defining a Document Type(SSC,Leaving)'''
    _name = "document.type"
    _description = "Document Type"
    _rec_name="doc_type"
    _order = "seq_no"
    _defaults={
        'seq_no': lambda obj, cr, uid, context:obj.pool.get('ir.sequence').get(cr, uid, 'document.type'),
    }
    _columns = {
        'seq_no': fields.char('Sequence', readonly=True),
        'doc_type': fields.char('Document Type', size=64, required=True),
    }


class Patient_Description(osv.Model):
    _name = 'patient.description'
    _columns = {
        'des_id':fields.many2one('patient.patient', 'Description'),
        'name':fields.char('Name', size=20),
        'description':fields.char('Description',size=50),
    }

class Patient_istory(osv.Model):
    _name = "patient.history"
    _columns = {
        'patient_id': fields.many2one('Patient.Patient', 'Patient'),
        'academice_year_id':fields.many2one('academic.year', 'Academic Year', required=True),
        'standard_id': fields.many2one('hospital.standard', 'Standard', required=True),
        'percentage': fields.float("Percentage", readonly=True ),
        'result': fields.char(string ='Result', readonly=True, method=True,type = 'char', store=True, size =30),
    }

class Patient_certificate(osv.Model):
    _name = "Patient.certificate"
    _columns = {
        'patient_id' : fields.many2one('Patient.Patient', 'Patient'),
        'description' : fields.char('Description',size=50),
        'certi' : fields.binary('Certificate',required =True)
    }

class res_partner(osv.Model):
    '''Defining a address information '''
    _inherit = 'res.partner'
    _description = 'Address Information'
    _columns = {
        'patient_id': fields.many2one('patient.patient','Patient')
    }

class Patient_Reference(osv.Model):
    ''' Defining a Patient reference information '''
    _name = "patient.reference"
    _description = "Patient Reference"
    _columns = {
        'reference_id': fields.many2one('Patient.Patient', 'Patient'),
        'name': fields.char('First Name', size=64, required=True),
        'middle': fields.char('Middle Name', size=64, required=True),
        'last': fields.char('Surname', size=64, required=True),
        'designation': fields.char('Designation', size=12, required=True),
        'phone': fields.char('Phone', size=12, required=True),
        'gender':fields.selection([('male','Male'), ('female','Female')], 'Gender'),
    }

class Patient_previous_hospital(osv.Model):
    ''' Defining a Patient previous hospital information '''
    _name = "Patient.previous.hospital"
    _description = "Patient Previous hospital"
    _columns = {
        'previous_hospital_id': fields.many2one('Patient.Patient', 'Patient'),
        'name': fields.char('Name', size=64, required=True),
        'registration_no': fields.char('Registration No.', size=12, required=True),
        'clinical_date': fields.date('Clinical Date'),
        'exit_date': fields.date('Exit Date'),
        'course_id':fields.many2one('standard.standard', 'Course', required=True),
    }

class Hospital_treatment(osv.Model):
    ''' Defining a Patient previous hospital information '''
    _name = "hospital.treatment"
    _description = "Patient Previous hospital treatment"
    _columns = {
        'add_sub_id': fields.many2one('Patient.previous.hospital', 'Add Subjects',invisible=True),
        'name': fields.char('Name', size=64, required=True),
        'maximum_marks': fields.integer("Maximum marks"),
        'minimum_marks': fields.integer("Minimum marks"),
    }

class Patient_family_contact(osv.Model):
    ''' Defining a Patient emergency contact information '''
    _name = "Patient.family.contact"
    _description = "Patient Family Contact"
    _columns = {
        'family_contact_id': fields.many2one('Patient.Patient', 'Patient'),
        'rel_name': fields.selection([('exist','Link to Existing Patient'), ('new','Create New Relative Name')], 'Related Patient', help="Select Name", required=True),
        'stu_name':fields.related('user_id','name',type='many2one',relation='Patient.Patient',string='Name',help="Select Patient From Existing List"),
        'name':fields.char('Name',size=20 ),
        'relation': fields.many2one('Patient.relation.master','Relation', required=True),
        'phone': fields.char('Phone', size=20, required=True),
        'email': fields.char('E-Mail', size=100),
    }

class Patient_relation_master(osv.Model):
    ''' Patient Relation Information '''
    _name = "Patient.relation.master"
    _description = "Patient Relation Master"
    _columns = {
                'name':fields.char('Name',size=20,required=True,help="Enter Relation name"),
                'seq_no':fields.integer('Sequence',size=5),
                }

class grade_master(osv.Model):
    _name = 'grade.master'
    _columns = {
        'name': fields.char('Grade', size = 256, select=1, required=True),
        'grade_ids':fields.one2many('grade.line','grade_id', 'Grade Name'),
    }

class grade_line(osv.Model):
    _name = 'grade.line'
    _columns = {
        'from_mark': fields.integer("From Marks",required=True, help="The grade will starts from this marks."),
        'to_mark': fields.integer('To Marks',required=True, help="The grade will ends to this marks."),
        'grade': fields.char('Grade', size = 8, required=True, help="Grade"),
        'sequence' : fields.integer('Sequence',help="Sequence order of the grade."),
        'fail': fields.boolean("Fail",help="If fail field is set to True, it will allow you to set the grade as fail."),
        'grade_id' : fields.many2one("grade.master",'Grade'),
        'name':fields.char('Name',size=15)
    }

class Patient_news(osv.Model):
    _name='Patient.news'
    _description = 'Patient News'
    _rec_name = 'subject'
    _columns={
        'subject' : fields.char('Subject', size=255, required=True, help="Subject of the news."), 
        'description' : fields.text('Description',help="Description"), 
        'date' :fields.datetime('Expiry Date',help="Expiry date of the news."),
        'user_ids' : fields.many2many('res.users','user_news_rel','id','user_ids','User News',help="Name to whom this news is related."),
        'color': fields.integer('Color Index'),
    }

    def news_update(self, cr, uid, ids, context = None):
        emp_obj = self.pool.get("hr.employee")
        obj_mail_server = self.pool.get('ir.mail_server')
        mail_server_ids = obj_mail_server.search(cr, uid, [], context=context)
        if not mail_server_ids:
            raise osv.except_osv(_('Mail Error'), _('No mail outgoing mail server specified!'))
        mail_server_record = obj_mail_server.browse(cr, uid, mail_server_ids[0])
        email_list = []
        for news in self.browse(cr, uid, ids, context):
            if news.user_ids:
                for user in news.user_ids:
                    if user.email:
                        email_list.append(user.email)
                if not email_list:
                    raise osv.except_osv(_('User Email Configuration '), _("Email not found in users !"))
            else:
                emp_ids = emp_obj.search(cr, uid, [], context = context)
                for employee in emp_obj.browse(cr, uid,emp_ids, context=context ):
                    if employee.work_email:
                        email_list.append(employee.work_email)
                    elif employee.user_id and employee.user_id.email:
                        email_list.append(employee.user_id.email)
                if not email_list:
                    raise osv.except_osv(_('Mail Error' ), _("Email not defined!")) 
            rec_date = fields.datetime.context_timestamp(cr, uid, datetime.strptime(news.date, DEFAULT_SERVER_DATETIME_FORMAT), context)
            body =  'Hi,<br/><br/> \
                This is a news update from <b>%s</b> posted at %s<br/><br/>\
                %s <br/><br/>\
                Thank you.' % (cr.dbname, rec_date.strftime('%d-%m-%Y %H:%M:%S'), news.description )
            message  = obj_mail_server.build_email(
                            email_from=mail_server_record.smtp_user, 
                            email_to=email_list, 
                            subject='Notification for news update.', 
                            body=body, 
                            body_alternative=body, 
                            email_cc=None, 
                            email_bcc=None, 
                            reply_to=mail_server_record.smtp_user, 
                            attachments=None, 
                            references = None, 
                            object_id=None, 
                            subtype='html', #It can be plain or html
                            subtype_alternative=None, 
                            headers=None)
            obj_mail_server.send_email(cr, uid, message=message, mail_server_id=mail_server_ids[0], context=context)
        return True

    _defaults = {
        'color': 0,
    }

class Patient_reminder(osv.Model):
    _name = 'Patient.reminder'
    _columns = {
        'stu_id': fields.many2one('Patient.Patient',' Patient Name',required = True),
        'name':fields.char('Title',size=30),
        'date' : fields.date('Date'),
        'description':fields.text('Description',size=130),
        'color': fields.integer('Color Index'),
    }
    _defaults = {
        'color': 0,
        }


class res_users(osv.Model):
     
    _inherit = 'res.users'
     
    def create(self, cr, uid, vals, context=None):
        vals.update({'employee_ids':False})
        res = super(res_users, self).create(cr, uid, vals, context=context)
        return res

# vim:expandtab:smartindent:tabstop=4:softtabstop=4:shiftwidth=4: