
#US-specific words
us_words={
    "keywords":
    """
    Federal Reserve, White House, 
    federal reserve, the fed, fed funds rate, Bernanke, Volcker, Greenspan, fed chairman, fed chair,
    Gramm-Rudman, 
    Medicaid, Medicare, FDA, part d, affordable care act, Obamacare,
    Medicaid, medicare, afdc, tanf, wic program, part d, oasdi, EITC,
    glass-steagall, tarp, dodd-frank, cftc, Volcker rule, sec, fdic, fslic, ots, occ, firrea, nlrb, davis-bacon, eeo, osha, ftc, epa
    """,
    }
    

#https://doi.org/10.1093/qje/qjw024
#https://www.policyuncertainty.com/
epu_categories={
    #baker2016 - pag 2 - newspapers that contain the following trio of terms
    "economic":
    """
    economy, economic
    """,
    

    #baker2016 - pag 17 - baseline policy term set for the EPU index in Figure I
    "policy":
    """
    regulation, deficit, Federal Reserve, White House, Congress, legislation
    """,
    

    #baker2016 - pag 2 - newspapers that contain the following trio of terms
    "uncertainty":
    """
    uncertainty, uncertain
    """,
    

    #https://www.policyuncertainty.com/categorical_terms.html
    "monetary_policy":
    """
    federal reserve, the fed, money supply, open market operations, quantitative easing, monetary policy, 
    fed funds rate, overnight lending rate, Bernanke, Volcker, Greenspan, central bank, 
    interest rates, fed chairman, fed chair, lender of last resort, discount window, European Central Bank, 
    ECB, Bank of England, Bank of Japan, BOJ, Bank of China, Bundesbank, Bank of France, Bank of Italy
    """,


    "taxes":
    """
    taxes, tax, taxation, taxed
    """,


    "fiscal_policy":
    """
    government spending, federal budget, budget battle, balanced budget, defense spending, 
    military spending, entitlement spending, fiscal stimulus, budget deficit, federal debt, national debt, 
    Gramm-Rudman, debt ceiling, fiscal footing, government deficits, balance the budget
    """,


    "health_care":
    """
    health care, Medicaid, Medicare, health insurance, malpractice tort reform, malpractice reform, 
    prescription drugs, drug policy, food and drug administration, FDA, medical malpractice, prescription drug act, 
    medical insurance reform, medical liability, part d, affordable care act, Obamacare
    """,


    "national_security":
    """
    national security, war, military conflict, terrorism, terror, 9/11, defense spending, military spending, 
    police action, armed forces, base closure, military procurement, saber rattling, naval blockade, 
    military embargo, no-fly zone, military invasion
    """,


    "entitlement_programs":
    """
    entitlement program, entitlement spending, government entitlements, social security, 
    Medicaid, medicare, government welfare, welfare reform, unemployment insurance, unemployment benefits, 
    food stamps, afdc, tanf, wic program, disability insurance, part d, oasdi, Supplemental Nutrition Assistance Program, 
    Earned Income Tax Credit, EITC, head start program, public assistance, government subsidized housing
    """,


    "regulation":
    """
    regulation, banking supervision, glass-steagall, tarp, bank supervision, thrift supervision, 
    dodd-frank, financial reform, commodity futures trading commission, cftc, house financial services committee, 
    basel, capital requirement, Volcker rule, bank stress test, securities and exchange commission, sec, deposit insurance, 
    fdic, fslic, ots, occ, firrea, truth in lending, union rights, card check, collective bargaining law, 
    national labor relations board, nlrb, minimum wage, living wage, right to work, closed shop, wages and hours, 
    workers compensation, advance notice requirement, affirmative action, at-will employment, overtime requirements, 
    trade adjustment assistance, davis-bacon, equal employment opportunity, eeo, osha, antitrust, competition policy, 
    merger policy, monopoly, patent, copyright, federal trade commission, ftc, unfair business practice, cartel, 
    competition law, price fixing, class action, healthcare lawsuit, tort reform, tort policy, punitive damages, 
    medical malpractice, energy policy, energy tax, carbon tax, cap and trade, cap and tax, drilling restrictions, 
    offshore drilling, pollution controls, environmental restrictions, clean air act, clean water act, 
    environmental protection agency, epa, immigration policy
    """,


    "financial_regulation":
    """
    banking supervision, glass-steagall, tarp, bank supervision, thrift supervision, dodd-frank, 
    financial reform, commodity futures trading commission, cftc, house financial services committee, basel, 
    capital requirement, Volcker rule, bank stress test, securities and exchange commission, sec, 
    deposit insurance, fdic, fslic, ots, occ, firrea, truth in lending
    """,


    "trade_policy":
    """
    import tariffs, import duty, import barrier, government subsidies, government subsidy, wto, 
    world trade organization, trade treaty, trade agreement, trade policy, trade act, doha round, uruguay round, 
    gatt, dumping
    """,


    "sovereign_debt":
    """
    sovereign debt, currency crisis, currency crash, currency devaluation, currency revaluation, 
    currency manipulation, euro crisis, Eurozone crisis, european financial crisis, european debt, asian financial crisis, 
    asian crisis, Russian financial crisis, Russian crisis, exchange rate
    """,
    }


#http://www.centralbanksguide.com/central+banks+list/
central_banks={
    "keywords":
    """
    Afghanistan Central Bank, Albania Central Bank, Algeria Central Bank, Argentina Central Bank, 
    Armenia Central Bank, Aruba Central Bank, Australia Central Bank, Austria Central Bank, 
    Azerbaijan Central Bank, Bahamas Central Bank, Bahrain Central Bank, Bangladesh Central Bank, 
    Barbados Central Bank, Belarus Central Bank, Belgium Central Bank, Belize Central Bank, 
    Benin Central Bank, Bermuda Central Bank, Bhutan Central Bank, Bolivia Central Bank, Bosnia Central Bank, 
    Botswana Central Bank, Brazil Central Bank, Bulgaria Central Bank, Burkina Faso Central Bank, 
    Cambodia Central Bank, Cameroon Central Bank, Canada Central Bank, Cayman Islands Central Bank, 
    Central African Republic Central Bank, Chad Central Bank, Chile Central Bank, China Central Bank, 
    Colombia Central Bank, Comoros Central Bank, Congo Central Bank, Costa Rica Central Bank, 
    Cote d'Ivoire Central Bank, Croatia Central Bank, Cuba Central Bank, Cyprus Central Bank, 
    Czech Republic Central Bank, Denmark Central Bank, Dominican Republic Central Bank, 
    East Caribbean area Central Bank, Ecuador Central Bank, Egypt Central Bank, El Salvador Central Bank, 
    Equatorial Guinea Central Bank, Estonia Central Bank, Ethiopia Central Bank, European Union Central Bank, 
    Fiji Central Bank, Finland Central Bank, France Central Bank, Gabon Central Bank, The Gambia Central Bank, 
    Georgia Central Bank, Germany Central Bank, Ghana Central Bank, Greece Central Bank, Guatemala Central Bank, 
    Guinea Bissau Central Bank, Guyana Central Bank, Haiti Central Bank, Honduras Central Bank, 
    Hong Kong Central Bank, Hungary Central Bank, Iceland Central Bank, India Central Bank, Indonesia Central Bank, 
    Iran Central Bank, Iraq Central Bank, Ireland Central Bank, Israel Central Bank, Italy Central Bank, 
    Jamaica Central Bank, Japan Central Bank, Jordan Central Bank, Kazakhstan Central Bank, Kenya Central Bank, 
    Korea Central Bank, Kuwait Central Bank, Kyrgyzstan Central Bank, Latvia Central Bank, Lebanon Central Bank, 
    Lesotho Central Bank, Libya Central Bank, Lithuania Central Bank, Luxembourg Central Bank, Macao Central Bank, 
    Macedonia Central Bank, Madagascar Central Bank, Malawi Central Bank, Malaysia Central Bank, Mali Central Bank, 
    Malta Central Bank, Mauritius Central Bank, Mexico Central Bank, Moldova Central Bank, Mongolia Central Bank, 
    Morocco Central Bank, Mozambique Central Bank, Namibia Central Bank, Nepal Central Bank, Netherlands Central Bank, 
    Netherlands Antilles Central Bank, New Zealand Central Bank, Nicaragua Central Bank, Niger Central Bank, 
    Nigeria Central Bank, Norway Central Bank, Oman Central Bank, Pakistan Central Bank, Papua New Guinea Central Bank, 
    Paraguay Central Bank, Peru Central Bank, Philippines Central Bank, Poland Central Bank, Portugal Central Bank, 
    Qatar Central Bank, Romania Central Bank, Russia Central Bank, Rwanda Central Bank, San Marino Central Bank, 
    Samoa Central Bank, Saudi Arabia Central Bank, Senegal Central Bank, Serbia Central Bank, Seychelles Central Bank, 
    Sierra Leone Central Bank, Singapore Central Bank, Slovakia Central Bank, Slovenia Central Bank, 
    Solomon Islands Central Bank, South Africa Central Bank, Spain Central Bank, Sri Lanka Central Bank, 
    Sudan Central Bank, Surinam Central Bank, Swaziland Central Bank, Sweden Central Bank, Switzerland Central Bank, 
    Tajikistan Central Bank, Tanzania Central Bank, Thailand Central Bank, Togo Central Bank, Tonga Central Bank, 
    Trinidad and Tobago Central Bank, Tunisia Central Bank, Turkey Central Bank, Uganda Central Bank, Ukraine Central Bank, 
    United Arab Emirates Central Bank, United Kingdom Central Bank, United States of America Central Bank, 
    Uruguay Central Bank, Vanuatu Central Bank, Venezuela Central Bank, Yemen Central Bank, Zambia Central Bank, 
    Zimbabwe Central Bank
    """,
    }
    