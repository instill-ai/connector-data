import jsonref
import json

from urllib.request import urlopen
from os.path import dirname


url = 'https://connectors.airbyte.com/files/registries/v0/oss_registry.json'
response = urlopen(url)
data_json = json.loads(response.read())

definitions = data_json['destinations']

name_set = set()
with open('./component.json', 'r') as compFile:
    comp = json.load(compFile)

for idx in range(len(definitions)):
    # print(definitions[idx].keys())

    definitions[idx]['uid'] = definitions[idx]['destinationDefinitionId']
    definitions[idx]['id'] = f"airbyte-{definitions[idx]['dockerRepository'].split('/')[1]}"
    definitions[idx]['title'] = definitions[idx]['name']

    definitions[idx]['vendor_attributes'] = {
        'dockerRepository': definitions[idx]['dockerRepository'],
        'dockerImageTag': definitions[idx]['dockerImageTag'],
        'releaseStage': definitions[idx]['releaseStage'],
        'tags': definitions[idx]['tags'],
        'license': definitions[idx]['license'],
        'githubIssueLabel': definitions[idx]['githubIssueLabel'],
        'sourceType': definitions[idx]['sourceType'],
        'resourceRequirements': definitions[idx].get('resourceRequirements', {}),
        'spec': {
            'supported_destination_sync_modes': definitions[idx]['spec']['supported_destination_sync_modes'],
        }
    }

    for to_moved in ['resourceRequirements', 'normalizationConfig', 'supportsDbt', 'ab_internal']:
        if to_moved in definitions[idx]:
            definitions[idx]['vendor_attributes'][to_moved] = definitions[idx][to_moved]
    for to_moved in ['supportsIncremental', 'supportsNormalization', 'supportsDBT', 'supported_destination_sync_modes',
                     'authSpecification', 'advanced_auth', 'supportsNamespaces', 'protocol_version', "$schema"]:
        if to_moved in definitions[idx]['spec']:
            definitions[idx]['vendor_attributes']['spec'][to_moved] = definitions[idx]['spec'][to_moved]

    for to_deleted in ['destinationDefinitionId', 'name', 'dockerRepository', 'dockerImageTag', 'releaseStage', 'ab_internal',
                       'tags', 'license', 'githubIssueLabel', 'sourceType', 'resourceRequirements', 'normalizationConfig', 'supportsDbt']:
        definitions[idx].pop(to_deleted, None)
    for to_deleted in ['supportsIncremental', 'supportsNormalization', 'supportsDBT', 'supported_destination_sync_modes',
                       'authSpecification', 'advanced_auth', 'supportsNamespaces', 'protocol_version', "$schema"]:
        definitions[idx]['spec'].pop(to_deleted, None)

    definitions[idx]['documentation_url'] = definitions[idx]['documentationUrl']
    definitions[idx]['icon_url'] = definitions[idx].get('iconUrl', "")
    definitions[idx]['spec']['resource_specification'] = definitions[idx]['spec']['connectionSpecification'] 
    definitions[idx]['spec']['component_specification'] = {
        "$ref": "component.json"
    }
    definitions[idx]['spec']['openapi_specifications'] = {
        "$ref": "openapi.json"
    }

    definitions[idx].pop('iconUrl', None)
    definitions[idx].pop('documentationUrl', None)
    definitions[idx]['spec'].pop('connectionSpecification', None)
    definitions[idx]['spec'].pop('documentationUrl', None)
    for key in definitions[idx].keys():
        # print(key)
        name_set.add(key)

definitions_json = json.dumps(definitions, indent=2, sort_keys=True)
definitions_json = definitions_json.replace(
    "airbyte_secret", "credential_field")

with open('./definitions.json', 'w') as out_file:
    out_file.write(definitions_json)



base_path = dirname(__file__)
base_uri = 'file://{}/'.format(base_path)

with open("./definitions.json") as schema_file:
    a= jsonref.loads(schema_file.read(), base_uri=base_uri, jsonschema=True)

with open('../definitions.json', 'w') as o:
    json.dump(a, o, indent=2)

# TODO: auto-generate openapi.json component.json from data.json
