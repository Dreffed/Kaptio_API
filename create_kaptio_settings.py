import json

settings = {}

settings['api'] = {}
settings['api']['baseurl'] = "kaptio-staging.herokuapp.com"
settings['api']['auth'] = {}
settings['api']['auth']['key'] = "<KEY>"
settings['api']['auth']['secret'] = "<SECRET>"

with open("kaptio_settings.json", "w") as f:  
    json.dump(settings, f, indent=4)