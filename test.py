

text="\\end{tabular} qwe "

#end tabular
old="\\end{tabular}"
new=(
    "\\end{tabular}"  "%"  "\n"
    "}" "\n"
    )
text=text.replace(old, new)

print(text)