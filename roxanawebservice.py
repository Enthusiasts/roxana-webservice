__author__ = 'debalid'

from flask import Flask, Blueprint, request, jsonify, send_from_directory
from entertainments import EntertainmentsDAO
from postgres import PostgresInjection
import json

#with PostgresInjection() as postgres:
postgres = PostgresInjection()

api = Blueprint("rona-webservice", __name__)


@api.route('/entertainment/', methods=['GET'])
def get_entertainment():
    if 'type' in request.args:
        ent_type = request.args["type"]
        ent_dao = EntertainmentsDAO(postgres)

        if 'cluster' in request.args:
            count, result = ent_dao.by_type_with_cluster_checkins(ent_type)
        else:
            count, result = ent_dao.by_type_with_photo(ent_type)

        #if 'photos' in request.args and request.args['photos'].lower() == 'true':
            #count, result = ent_dao.by_type_with_photo(ent_type)
        #else:
            #count, result = ent_dao.by_type(ent_type)
        return jsonify(length=count, results=result), 200

    else:
        return 404


@api.route('/entertainment/clusterinfo/')
def get_entertainment_clusterinfo():
    if 'label' in request.args:
        label = request.args["label"]
        ent_dao = EntertainmentsDAO(postgres)

        if label.lower() == "checkins":
            count, result = ent_dao.cluster_checkins_info()
        else:
            count, result = 0, []

        return jsonify(length=count, results=result), 200

    else:
        return 404

@api.route('/static/<path:path>', methods=['GET'])
def get_static(path):
    return send_from_directory("static", path)


app = Flask(__name__)
app.register_blueprint(api, url_prefix="/rona/api")


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

if __name__ == '__main__':
    app.run(debug=True)
