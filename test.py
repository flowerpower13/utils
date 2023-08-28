inputs = [
    {
        "fixedeffects": [
            {"name": "IndustryFE",  "present": "Yes", "prefix": "industry_ff_dummy"}, 
            {"name": "YearFE",      "present": "Yes", "prefix": "year_dummy"},  
            {"name": "FirmFE",      "present": "No", "prefix": "firm_dummy"}, 
        ]
    },

    {
        "fixedeffects": [
            {"name": "IndustryFE",  "present": "Yes", "prefix": "industry_ff_dummy"}, 
            {"name": "YearFE",      "present": "No", "prefix": "year_dummy"},  
            {"name": "FirmFE",      "present": "No", "prefix": "firm_dummy"}, 
        ]
    },
]

#init
new_dictfe=dict()

#for
for j, input in enumerate(inputs):

    #list fe
    list_fe=input["fixedeffects"]
    
    #for
    for k, dict_fe in enumerate(list_fe):

        #unpack
        name=dict_fe["name"]
        present=dict_fe["present"]

        if name not in new_dictfe:

            #gen
            new_dictfe[name]=[None]*len(inputs)



        #update
        new_dictfe[name][j]=present

print(new_dictfe)



