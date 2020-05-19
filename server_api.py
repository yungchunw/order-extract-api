import os
import main
import magic
import uuid
from flask import Flask, request, redirect, url_for, jsonify
from werkzeug.utils import secure_filename


ALLOWED_EXTENSIONS = {'pdf', 'PDF'}
ALLOWED_MIME_TYPES = {'application/pdf'}


app = Flask(__name__)
app.debug=True
app.config['UPLOADS_PATH'] = './upload_pdf'

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
        
        f = request.files["data"]
        prefix_id = request.args.get('prefix_id')

        if f and is_allowed_file(f):
            # creating a pdf file object 
            pdf_name = secure_filename(f.filename)
            pdf_path = os.path.join(app.config['UPLOADS_PATH'],pdf_name)
            parse_id = str(uuid.uuid4())[:8]

            print("{} - {} - File recieved, start processing.".format(parse_id, pdf_name))
            
            f.stream.seek(0)
            f.save(pdf_path)

            try:
                if prefix_id is None:
                    prefix_id = pdf_name.split('_')[0]

                result = main.process(pdf_path, prefix_id)

                print("{} - {} - Process successfully completed.".format(parse_id, pdf_name))
            
                resp = jsonify(result)
                resp.status_code = 200

            except Exception as e:
                print("{} - {} - Something wrong. Details: {}".format(parse_id, pdf_name,e))
                message = {
                    'status': 500,
                    'message': 'Undefine error found. Please check log for details.'
                }
                resp = jsonify(message)
                resp.status_code = 500

        else:
            print("No file uploaded!")
            message = {
                    'status': 400,
                    'message': 'No file uploaded.'
            }
            resp = jsonify(message)
            resp.status_code = 400

        return resp
        
    else:
        print("Wrong request type!")
        
    

if __name__ == "__main__":
    app.run()