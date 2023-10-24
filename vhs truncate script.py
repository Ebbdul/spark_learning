import psycopg2
import pandas as pd

try:
    def trunc():
        conn = psycopg2.connect(
            host="192.168.2.48",
            port='5058',
            database="wasay",
            user="postgres",
            password="Red*St0ne")
        t_names=['bank',	'bank_history',	'comment',	'comment_history',	'label_constants',	'constant_language',	'insurance_provider_history',	'insurance_provider',	'diagnosis_history',	'diagnosis',	'holidays',	'invoice_history',	'invoice_detail_history',	'invoice_detail',	'invoice',	'login_security_questions',	'medicine_history',	'medicine',	'patient_allergy_history',	'patient_allergy',	'patient_consultation_history',	'patient_consultation',	'patient_disability_history',	'patient_diagnosis',	'patient_document_history',	'patient_document',	'patient_followup_history',	'patient_followup',	'patient_immunization_history',	'patient_immunization',	'patient_insurance_history',	'patient_insurance',	'patient_leave_note_history',	'patient_leave_note',	'patient_medical_report_template_history',	'patient_medical_report_template',	'patient_medical_report_history',	'patient_medical_report',	'patient_medicine_history',	'patient_medicine',	'patient_procedure_history',	'patient_procedure',	'patient_payment_history',	'patient_payment',	'patient_social_history_history',	'patient_social_history',	'patient_payment_history',	'patient_payment',	'patient_referral_history',	'physician_work_experiencetest',	'physician_work_experience_historytest',	'physician_work_experience_history',	'Physician_work_experience']
        cursor = conn.cursor()
        # cursor.execute('select * from db_vhs.public.users')
        # for i in cursor.fetchall():
        #     print(i)
        for t in t_names:
            script="truncate table "+t+" cascade;"
            # print(script)
            cursor.execute(str(script))
            print('Table',t,'Truncated!')


# user_id = 973  should not be delete
        conn.commit()
        conn.close()
        # return "Success"


    trunc()
except Exception as e:
    print(e)







