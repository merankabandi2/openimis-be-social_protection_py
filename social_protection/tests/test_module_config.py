import json
from core.models import ModuleConfiguration
from django.test import TestCase
from social_protection.apps import SocialProtectionConfig


class ModuleConfigTest(TestCase):

    def test_config_reloading(self):
        # First set the individual config to be empty
        config = ModuleConfiguration.objects.filter(module='social_protection', layer='be')
        if not config:
            config = ModuleConfiguration(module='social_protection', layer='be', config='{}')
        else:
            config.config = '{}'
        config.save()

        self.assertTrue(SocialProtectionConfig.gql_check_benefit_plan_update)
        self.assertTrue(SocialProtectionConfig.enable_maker_checker_logic_enrollment)

        # Update config should trigger a reload
        updated_config = {
          "gql_check_benefit_plan_update": False,
          "enable_maker_checker_logic_enrollment": False,
        }
        config.config = json.dumps(updated_config)
        config.save()

        self.assertFalse(SocialProtectionConfig.gql_check_benefit_plan_update)
        self.assertFalse(SocialProtectionConfig.enable_maker_checker_logic_enrollment)

