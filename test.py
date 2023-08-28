import pandas as pd

# Create a sample panel data DataFrame
data = {
    'firm': ['A', 'B', 'C', 'A', 'B'],
    'year': [2019, 2019, 2019, 2020, 2020],
    'dummy': [1, 1, 0, 0, 0],  # Updated to have some 0s
    'industry_code': [123, 456, 789, 123, 789]
}

panel_data = pd.DataFrame(data)

# Display the original panel data DataFrame
print("Original Panel Data:")
print(panel_data)

# Calculate industry-wise dummy value counts
industry_counts = panel_data.groupby('industry_code')['dummy'].value_counts().unstack(fill_value=0)

# Identify industry codes with only dummy=0
industries_to_drop = industry_counts.index[industry_counts[1] == 0]

# Drop observations with industry codes in 'industries_to_drop'
filtered_panel_data = panel_data[~panel_data['industry_code'].isin(industries_to_drop)]

# Reset index
filtered_panel_data.reset_index(drop=True, inplace=True)

# Display the filtered panel data DataFrame
print("\nFiltered Panel Data:")
print(filtered_panel_data)
