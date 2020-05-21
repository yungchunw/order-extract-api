import os
import main
import magic
import uuid
from flask import Flask, request, redirect, url_for, jsonify
from werkzeug.utils import secure_filename
from util_lib import log_util

ALLOWED_EXTENSIONS = {'pdf', 'PDF'}
ALLOWED_MIME_TYPES = {'application/pdf'}
PATH = {'UPLOADS':'./upload_pdf',
        'TEMP':'./tmp',
        'OUTPUT':'./output',
        'FINAL':'./Final_Json'}

app = Flask(__name__)

def check_path():
    for key, value in PATH.items():
        if not os.path.exists(value):
            os.mkdir(value)

def is_allowed_file(file):
    if '.' in file.filename:
        ext = file.filename.rsplit('.', 1)[1].lower()
    else:
        return False

    mime_type = magic.from_buffer(file.stream.read(), mime=True)
    if (
        mime_type in ALLOWED_MIME_TYPES and
        ext in ALLOWED_EXTENSIONS
    ):
        return True

    return False


@app.route('/process', methods=['POST'])
def process():

    if request.method == "POST":
        check_path()
        f = request.files["data"]
        prefix_id = request.args.get('prefix_id')
        # creating a pdf file object 
        pdf_name = secure_filename(f.filename)
        pdf_path = os.path.join(PATH['UPLOADS'],pdf_name)
        parse_id = str(uuid.uuid4())[:8]

        extras = {'pdfname':pdf_name, 'parse_id':parse_id}

        if f and is_allowed_file(f):
            
            log_util.logger.debug("File recieved, start processing.", extra=extras)
            
            f.stream.seek(0)
            f.save(pdf_path)

            try:
                if prefix_id is None:
                    prefix_id = pdf_name.split('_')[0]

                result = main.process(pdf_path, prefix_id, extras)

                log_util.logger.debug("Process successfully completed.", extra=extras)
            
                resp = jsonify(result)
                resp.status_code = 200

            except Exception as e:
                log_util.logger.error("Something wrong. Details: {}".format(e), extra=extras)
                message = {
                    'status': 500,
                    'message': 'Undefine error found. Please check log for details.'
                }
                resp = jsonify(message)
                resp.status_code = 500

        else:
            log_util.logger.warning("No file uploaded!", extra=extras)
            message = {
                    'status': 400,
                    'message': 'No file uploaded.'
            }
            resp = jsonify(message)
            resp.status_code = 400

        return resp
        
    else:
        log_util.logger.warning("Wrong request type!", extra=extras)
        
    

if __name__ == "__main__":
    from werkzeug.contrib.fixers import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app)
    # log_util.logger.info("Gunicorn servcie activate!")
    app.run(host = '0.0.0.0', use_reloader = False,debug=True, threaded=True)