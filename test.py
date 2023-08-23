
caption=""
label=""
tabular="ok"
text=(
    "\\begin{table}[!htbp]" + "\n"
    "\\centering" + "\n"
    f"\\caption{caption}" + "\n"
    f"\\label{label}" + "\n"
    "\\resizebox{\\textwidth}{!}{%" + "\n"
    f"{tabular}" + "\n"
    "" + "\n"
    "" + "\n"
    "" + "\n"
    )

print(text)