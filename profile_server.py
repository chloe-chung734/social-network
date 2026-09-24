##Profile server connections
## client -> profile server
## actions: profile requests (create profile? view profiles)
## return: profiles

## profile server -> database
## actions: store profiles
## return: retrieve profiles


from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)


# "table" to store profiles -- eventually swap this for database connection
_profiles = {}

def store_profile(data: dict) -> dict:
    ## stores profile in database
    
    ##check the data for existing user_id
    user_id = data["user_id"]
    
    ##check if already exists
    existing = _profiles.get(user_id, {})
    
    ##store with profile attributes
    profile = {
        "user_id": user_id,
        "username": data.get("username", existing.get("username")),
        "display_name": data.get("display_name", existing.get("display_name")),
        "bio": data.get("bio", existing.get("bio", ""))
        }
    
    ##assign profile at position user_id to new profile
    _profiles[user_id] = profile
    return profile

def retrieve_profile(user_id: str):
    ## retruns profile from database or none if does not exist
    return _profiles.get(user_id)



##following processes talk to client

##assign decorator get method using user_id
@app.route("/profiles/<user_id>", methods=["GET"])
def get_profile(user_id):
    ## processes 'retrieves profile' request
    
    ##retrieve profile from database
    profile = retrieve_profile(user_id)
    ##check if exists
    if profile is None:
        return ({"error": "profile not found"})
    return profile

##assign post method route
@app.route("/profiles/<user_id>", methods=["POST"])
def create_profile():
    ## processes 'create profile' request
    
    ## data = request, no request returns None
    data = request.get_json(silent=True)
    
    ## if no request info or missing data -> error
    if not data or "user_id" not in data:
        return ({"error": "user_id is required"})
    
    ## if user_id already taken -> error
    if retrieve_profile(data["user_id"]) is not None:
        return ({"error": "profile already exists"})
    
    ## store new profile in database
    profile = store_profile(data)
    
    ## return profile
    return profile

##assign update profile route, methods to put (replace all) or patch (replace some)
@app.route("/profiles/<user_id>", methods=["PUT", "PATCH"])
def update_profile(user_id):
    ## processes 'update profile' request
    
    ## data = request received, no request returns None
    data = request.get_json(silent=True)
    
    ## if update data missing -> error
    if not data:
        return ({"error":"no update data provided"})
    
    ## if user_id not found -> error
    if retrieve_profile(user_id) is None:
        return ({"error": "profile not found"})
    
    ## assign profile at user_id to updated profile at user_id
    data["user_id"] = user_id
    
    ## store updated profile in database
    profile = store_profile(data)
    return profile