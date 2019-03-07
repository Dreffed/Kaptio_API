import json

settings = {}

settings['auth'] = {}
settings['auth']['key'] = "<KEY>"
settings['auth']['secret'] = "<SECRET>"

settings['api'] = {}
settings['api']['baseurl'] = "kaptio-staging.herokuapp.com"

with open("kaptio_settings.json", "w") as f:  
    json.dump(settings, f, indent=4)