
#variables
from _dicts_epu import us_words, epu_categories, central_banks
from _dicts_biodiversity import unepwcmc_single, unepwcmc_multiple, cbdint_single, cbdint_multiple, BBCglossary_single, BBCglossary_multiple
from _dicts_mfd import care, fairness, ingroup, authority


dict_topics={
    #EPU
    "epu_economic": {
    "good_keywords": epu_categories["economic"],
    "bad_keywords": us_words["keywords"],
    },  

    "epu_policy": {
    "good_keywords": epu_categories["policy"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_uncertainty": {
    "good_keywords": epu_categories["uncertainty"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_monetary_policy": {
    "good_keywords": epu_categories["monetary_policy"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_taxes": {
    "good_keywords": epu_categories["taxes"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_fiscal_policy": {
    "good_keywords": epu_categories["fiscal_policy"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_health_care": {
    "good_keywords": epu_categories["health_care"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_national_security": {
    "good_keywords": epu_categories["national_security"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_entitlement_programs": {
    "good_keywords": epu_categories["entitlement_programs"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_regulation": {
    "good_keywords": epu_categories["regulation"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_entitlement_programs": {
    "good_keywords": epu_categories["entitlement_programs"],
    "bad_keywords": us_words["keywords"],
    },
    
    "epu_financial_regulation": {
    "good_keywords": epu_categories["financial_regulation"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_sovereign_debt": {
    "good_keywords": epu_categories["sovereign_debt"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_financial_regulation": {
    "good_keywords": epu_categories["financial_regulation"],
    "bad_keywords": us_words["keywords"],
    },

    "epu_central_banks": {
    "good_keywords": central_banks["keywords"],
    "bad_keywords": us_words["keywords"],
    },


    #BIODIVERSITY
    "biodiversity_unepwcmc_single": {
    "good_keywords": unepwcmc_single["keywords"],
    "bad_keywords": "no_bad_keywords",
    },
    #add other biodiversity dicts?


    #MFT
    "mft_care_virtue": {
    "good_keywords": care["virtue"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_care_vice": {
    "good_keywords": care["vice"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_fairness_virtue": {
    "good_keywords": fairness["virtue"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_fairness_vice": {
    "good_keywords": fairness["vice"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_ingroup_virtue": {
    "good_keywords": ingroup["virtue"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_ingroup_vice": {
    "good_keywords": ingroup["vice"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_authority_virtue": {
    "good_keywords": authority["virtue"],
    "bad_keywords": "no_bad_keywords",
    },

    "mft_authority_vice": {
    "good_keywords": authority["vice"],
    "bad_keywords": "no_bad_keywords",
    },

    }
#'''