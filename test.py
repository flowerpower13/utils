# Sample text with 100 words
text = "Your text goes here, which contains some instances of sovereign debt. This is a sample text for demonstration purposes. Sovereign debt refers to the debt incurred by a country or a government. In recent news, there have been discussions about the implications of sovereign debt on the global economy. The management of sovereign debt is a critical aspect of financial stability."

# Search for instances of "sovereign debt" and extract 200 characters before and after
target_phrase = "sovereign debt"
result = []

index = 0

while index < len(text):
    index = text.find(target_phrase, index)
    if index == -1:
        break
    start_index = max(0, index - 200)
    end_index = min(index + len(target_phrase) + 200, len(text))
    extracted_text = text[start_index:end_index]
    result.append(extracted_text)
    index += len(target_phrase)


print(result)