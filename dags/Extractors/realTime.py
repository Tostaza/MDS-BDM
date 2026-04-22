import json
import os
from time import sleep
import requests
import subprocess
from .AExtractor import Extractor
from Extractors.xml import ExtractorXML
from Extractors.json import ExtractorJSON
from datetime import datetime
import time

BASE_DIR = os.path.dirname(__file__)
RUTA_JSON = os.path.join(BASE_DIR, "..", "JSON", "points.json")


class ExtractorRealTime(Extractor):
    
    def load_points(self):
        with open(RUTA_JSON, "r") as f:
            return json.load(f)["points"]
        
    def calculate_congestion(self, flow):
        try:
            current = flow.get("currentSpeed", 1)
            free = flow.get("freeFlowSpeed", 1)
            ratio = current / free
            if ratio > 0.8:
                return "low"
            elif ratio > 0.5:
                return "medium"
            elif ratio > 0.3:
                return "high"
            return "very_high"
        except:
            return "unknown"
        
    def calculate_delay(self, route):
        try:
            duration = float(route.get("duration", "0s").replace("s", ""))
            static = float(route.get("staticDuration", "0s").replace("s", ""))
            return duration - static
        except:
            return 0

    def extract_data(self, api_endpoint, params=None, headers=None):
        pass

    def extract_real_time_data(self, api_endpoint, type, api_key : str = None):
        if type == "TomTom":
            if api_key is None:
                print("Error, missing API KEY FOR TOM TOM")
                return
            return self.extract_data_Tom_Tom(api_endpoint, api_key)
        elif type == "Google":
            return self.extract_data_google(api_endpoint)
        elif type == 'xml':
            extractor = ExtractorXML()
            return extractor.extract_data(api_endpoint)
        elif type == "json":
            extractor = ExtractorJSON()
            return extractor.extract_data(api_endpoint)
        elif type == "python_code":
            return self.run_python_producer()
        return None, None


    def run_python_producer(self):
        script_path = os.path.join(os.path.dirname(__file__), "..", "IoT", "sensor_bcn.py")
        try:
            subprocess.Popen(["python", script_path])
            return None, "producer_started"
        except Exception as e:
            print(f"Error launching producer: {e}")
            raise


    def extract_data_Tom_Tom(self, api_data, api_key):
        points = self.load_points()
        results = []

        for p in points:
            time.sleep(0.1)
            params = {
                "key": api_key,
                "point": f"{p['lat']},{p['lon']}",
                "unit": "KMPH"
            }
            try:
                response = requests.get(api_data, params=params)
                if response.status_code != 200:
                    continue
                data = response.json()
                flow = data.get("flowSegmentData", {})
                results.append({
                    "id": p["id"],
                    "name": p["name"],
                    "lat": p["lat"],
                    "lon": p["lon"],
                    "timestamp": datetime.now().isoformat(),
                    "current_speed": flow.get("currentSpeed"),
                    "free_flow_speed": flow.get("freeFlowSpeed"),
                    "confidence": flow.get("confidence"),
                    "road_closure": flow.get("roadClosure"),
                    "congestion_level": self.calculate_congestion(flow)
                })
            except Exception as e:
                print(f"Error amb punt {p['id']}: {e}")
        return results, "tomtom_realtime.json"

    def extract_data_google(self, api_data):
        points = self.load_points()
        results = []

        for i in range(len(points) - 1):
            time.sleep(0.1)
            origin = points[i]
            dest = points[i + 1]
            headers = {
                "X-Goog-Api-Key": "TOKEN",
                "Content-Type": "application/json"
            }
            body = {
                "origin": {
                    "location": {
                        "latLng": {
                            "latitude": origin["lat"],
                            "longitude": origin["lon"]
                        }
                    }
                },
                "destination": {
                    "location": {
                        "latLng": {
                            "latitude": dest["lat"],
                            "longitude": dest["lon"]
                        }
                    }
                },
                "travelMode": "DRIVE",
                "routingPreference": "TRAFFIC_AWARE"
            }
            try:
                response = requests.post(api_data, headers=headers, json=body)
                if response.status_code != 200:
                    continue
                data = response.json()
                route = data.get("routes", [{}])[0]
                results.append({
                    "origin_id": origin["id"],
                    "destination_id": dest["id"],
                    "origin_name": origin["name"],
                    "destination_name": dest["name"],
                    "duration": route.get("duration"),
                    "static_duration": route.get("staticDuration"),
                    "traffic_delay": self.calculate_delay(route)
                })
            except Exception as e:
                print(f"Error ruta {origin['id']} → {dest['id']}: {e}")
        return results, "google_routes.json"