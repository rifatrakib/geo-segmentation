from flask import Flask, render_template, request, jsonify
import utils

app = Flask(__name__)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/generate-segment-data', methods=['POST'])
def generate_segment_data():
    payload = request.json
    bounding_box = payload.get('bounding_box', None)
    view_name = payload.get('view_name', None)
    fields = get_filter_fields(view_name)
    
    if bounding_box and view_name:
        response = utils.retrieve_object_style_segment_data(bounding_box, view_name, fields, from_api=True)
    else:
        response = {'message': 'Must provide bounding_box in the payload'}
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True)
