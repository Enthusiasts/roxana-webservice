__author__ = 'debalid'

from flask import Flask, request, jsonify
from entertainments import EntertainmentsDAO
from postgres import PostgresInjection
import json

with PostgresInjection() as postgres:
#postgres = PostgresInjection()

    app = Flask(__name__)


    @app.route('/entertainment/', methods=['GET'])
    def get_entertainment():
        if 'type' in request.args:
            ent_type = request.args["type"]
            ent_dao = EntertainmentsDAO(postgres)
            count, result = ent_dao.by_type(ent_type)
            return jsonify(length=count, results=result), 200


    if __name__ == '__main__':
        app.run(debug=True)
