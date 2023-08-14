



rows=[
    "12-",
    "mdl",
    ]


for i, row in enumerate(rows):

    contains_digit=any(char.isdigit() for char in row)

    print("row:", row)
    print("contains_digit:", contains_digit)

    if not contains_digit:
        
        print("not")


    elif contains_digit:
        
        print("yes")