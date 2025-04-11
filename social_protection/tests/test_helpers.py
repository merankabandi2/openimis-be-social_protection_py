import random
import string
import copy
from individual.models import Individual, Group, GroupIndividual
from location.models import Location
from social_protection.models import BenefitPlan, Activity
from social_protection.tests.data import (
    service_add_payload_valid_schema,
    service_beneficiary_add_payload,
    service_add_individual_payload_with_ext,
)


def generate_random_string(length=6):
    letters = string.ascii_uppercase
    return ''.join(random.choice(letters) for i in range(length))

def merge_dicts(original, override):
    updated = copy.deepcopy(original)
    for key, value in override.items():
        if isinstance(value, dict) and key in updated:
            updated[key] = merge_dicts(updated.get(key, {}), value)
        else:
            updated[key] = value
    return updated

def create_benefit_plan(username, payload_override={}):
    updated_payload = merge_dicts(service_add_payload_valid_schema, payload_override)
    benefit_plan = BenefitPlan(**updated_payload)
    benefit_plan.save(username=username)

    return benefit_plan

def find_or_create_benefit_plan(payload, username):
    qs = BenefitPlan.objects.filter(**payload)
    if qs:
        return qs.first()
    else:
        return create_benefit_plan(username, payload)

def create_individual(username, payload_override={}):
    updated_payload = merge_dicts(service_add_individual_payload_with_ext, payload_override)
    individual = Individual(**updated_payload)
    individual.save(username=username)

    return individual

def create_group(username, payload_override={}):
    updated_payload = merge_dicts({'code': generate_random_string()}, payload_override)
    group = Group(**updated_payload)
    group.save(username=username)
    return group

def add_individual_to_group(username, individual, group, is_head=True):
    object_data = {
        "individual_id": individual.id,
        "group_id": group.id,
    }
    if is_head:
        object_data["role"] = "HEAD"
    group_individual = GroupIndividual(**object_data)
    group_individual.save(username=username)
    return group_individual

def create_group_with_individual(username, group_override={}, individual_override={}):
    individual = create_individual(username, individual_override)
    group = create_group(username, group_override)
    group_individual = add_individual_to_group(username, individual, group)
    return individual, group, group_individual

def add_individual_to_benefit_plan(service, individual, benefit_plan, payload_override={}):
    payload = {
        **service_beneficiary_add_payload,
        "individual_id": individual.id,
        "benefit_plan_id": benefit_plan.id,
        "json_ext": individual.json_ext,
    }
    benefit_plan.type = BenefitPlan.BenefitPlanType.INDIVIDUAL_TYPE
    updated_payload = merge_dicts(payload, payload_override)
    result = service.create(updated_payload)
    assert result.get('success', False), result.get('detail', "No details provided")
    uuid = result.get('data', {}).get('uuid', None)
    return uuid

def add_group_to_benefit_plan(service, group, benefit_plan, payload_override={}):
    payload = {
        **service_beneficiary_add_payload,
        "group_id": group.id,
        "benefit_plan_id": benefit_plan.id,
        "json_ext": group.json_ext,
    }
    benefit_plan.type = BenefitPlan.BenefitPlanType.GROUP_TYPE
    updated_payload = merge_dicts(payload, payload_override)
    result = service.create(updated_payload)
    assert result.get('success', False), result.get('detail', "No details provided")
    uuid = result.get('data', {}).get('uuid', None)
    return uuid

def find_or_create_activity(name, username):
    activity_found = Activity.objects.filter(name=name)
    if activity_found:
        activity = activity_found.first()
    else:
        activity = Activity(name=name)
        activity.save(username=username)
    return activity

