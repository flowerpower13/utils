
from nltk import tokenize
from pytrie import StringTrie


from _string_utils import _create_wordgraph, _wordbag_in_wordgraph, _find_from_prefix_trie


#test   
text="io sono andato a matera, io sono andato a Santeramo, io sono,  io sonale"
list_tokens=tokenize.word_tokenize(text)
word_graph=_create_wordgraph(list_tokens)

word_bags=[
    ("andat*", 2),
    ("materx*", 0),
    ("mater*", 1),
    ("son*", 4),
    ("io sono vito", 0), 
    ("io", 4),
    ]

prefix_trie=StringTrie()
for key in word_graph.keys():
    prefix_trie[key] = key

marker="*"
for word_bag, result in word_bags:
    if not word_bag.endswith(marker):
        tokens=tokenize.word_tokenize(word_bag)
        b=_wordbag_in_wordgraph(tokens, word_graph)
        print("b - ", b)
        if b!=result:
            print(f"error - {word_bag}")
    else:
        b=_find_from_prefix_trie(word_bag, word_graph, prefix_trie)
        x=sum(b.values())
        print("x - ", x)
        if not x==result:
            print(f"error - {word_bag}")

            

    



