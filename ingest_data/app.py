from flask import Flask, request, jsonify

# from dotenv import load_dotenv
# load_dotenv()

import src.flights as flights

app = Flask(__name__)

@app.route('/raw_flights_offers', methods=['GET'])
def app_get_flights_solo():
    origin = request.args.get('origin')
    destination = request.args.get('destination')
    departure_date = request.args.get('departure_date')

    # Validate query parameters
    if not all([origin, destination, departure_date]):
        return jsonify({"error": "Missing required parameters"}), 400

    # Get flights data
    try:
        flights_data = flights.get_flights_amadeus(
            origin=origin,
            destination=destination,
            departure_date=departure_date,
            currency_code="USD",
            save_to_gcs=True
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Return flights data
    return jsonify(flights_data), 200

if __name__ == "__main__":
    app.run(debug=False, host='0.0.0.0', port=8080)