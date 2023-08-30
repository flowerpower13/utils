import pandas as pd
import numpy as np
import statsmodels.formula.api as smf

# Create a DataFrame with fixed values for demonstration
data = pd.DataFrame({
    'dependent_variable': np.random.randn(99),  # Corrected to match length
    'independent_variable': np.random.randn(99),  # Corrected to match length
    'category_column': ['A', 'B', 'C'] * 33  # Repeats 'A', 'B', 'C' for demonstration
})

# Specify the formula for the model
formula_with_dummies = 'dependent_variable ~ independent_variable + C(category_column)'

# Fit the regression model with fixed effects using dummy variables
model_with_dummies = smf.ols(formula_with_dummies, data=data).fit()

# Using C(category_column) in the formula
formula_with_C = 'dependent_variable ~ independent_variable + C(category_column)'
model_with_C = smf.ols(formula_with_C, data=data).fit()

# Compare results
print("Model with dummy variables:")
print(model_with_dummies.summary())

print("\nModel using C(category_column) in the formula:")
print(model_with_C.summary())
