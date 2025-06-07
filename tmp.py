from transformers import pipeline

# Load the classification pipeline with the specified model
pipe = pipeline(
    "text-classification", model="tabularisai/multilingual-sentiment-analysis"
)

# Classify a new sentence
sentence = "I dont really like this product! WIsh it had an extra feature or two tho..."
result = pipe(sentence)

# Print the result
print(result)
