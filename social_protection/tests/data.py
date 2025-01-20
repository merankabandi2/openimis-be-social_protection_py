from core.datetimes.ad_datetime import datetime

service_add_payload = {
    "code": "example",
    "name": "example_name",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "$schema": "https://json-schema.org/draft/2019-09/schema"
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_add_payload_same_code = {
    "code": "example",
    "name": "random",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "$schema": "https://json-schema.org/draft/2019-09/schema"
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_add_payload_same_name = {
    "code": "random",
    "name": "example_name",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "$schema": "https://json-schema.org/draft/2019-09/schema"
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_add_payload_invalid_schema = {
    "code": "random",
    "name": "example_name",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "maxLength": "abc"
            },
            "age": {
                "type": "integer",
                "maximum": -10
            }
        }
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_add_payload_valid_schema = {
    "code": "example1",
    "name": "With Schema",
    "max_beneficiaries": 20,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
      "$schema": "http://json-schema.org/draft-04/schema#",
      "properties": {
        "email": {
          "type": "string",
        },
        "able_bodied": {
          "type": "boolean",
        },
        "number_of_children": {
          "type": "integer",
        }
      }
    },
    "json_ext": {
      'advanced_criteria': {
        'POTENTIAL': [
          {
            'type': 'integer',
            'field': 'number_of_children',
            'value': '1',
            'filter': 'gt'
          }
        ],
        'ACTIVE': [
          {
            'type': 'boolean',
            'field': 'able_bodied',
            'value': 'False',
            'filter': 'exact'
          }
        ]
      }
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_add_individual_payload_with_ext = {
    'first_name': 'Foo',
    'last_name': 'Bar',
    'dob': datetime.now(),
    'json_ext': {
        'email': 'foo.bar@example.com',
        'able_bodied': True,
        'number_of_children': 2,
    }
}

service_add_payload_no_ext = {
    "code": "example",
    "name": "example_name",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "$schema": "https://json-schema.org/draft/2019-09/schema"
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_update_payload = {
    "code": "update",
    "name": "example_update",
    "max_beneficiaries": 0,
    "ceiling_per_beneficiary": "0.00",
    "beneficiary_data_schema": {
        "$schema": "https://json-schema.org/draft/2019-09/schema"
    },
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_beneficiary_add_payload = {
    "status": "POTENTIAL",
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}

service_beneficiary_update_status_active_payload = {
    "status": "ACTIVE",
    "date_valid_from": "2023-01-01",
    "date_valid_to": "2023-12-31",
}
