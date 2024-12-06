import csv
import re

def parse_company_size(size_str):
    # size_str looks like "201-500 employees"
    # Extract the numeric range and take the midpoint
    match = re.match(r'(\d+)-(\d+)', size_str)
    if match:
        low = int(match.group(1))
        high = int(match.group(2))
        return (low + high) // 2
    plus_match = re.match(r'(\d+)\+ employees', size_str)
    if plus_match:
        # Return the base if it's "5000+ employees" → 5000
        base = int(plus_match.group(1))
        return base
    return None

def parse_founded_year(year_str):
    if year_str.strip():
        return int(year_str.strip())
    return None

def parse_currency_field(value_str):
    # Extract numeric and unit (M or B)
    # Return (amount_in_millions, original_unit)
    # If not found, return (None, None)
    if not value_str or value_str.strip() == '':
        return None, None
    match = re.search(r'\$([\d\.]+)([MB])', value_str)
    if match:
        amount = float(match.group(1))
        unit = match.group(2)  # 'M' or 'B'
        if unit == 'B':
            # Convert to millions
            amount = amount * 1000.0
        return amount, unit
    return None, None

input_file = 'sf_bay_area_startups.csv'
output_file = 'sf_bay_area_startups_cleaned.csv'

with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames

    # Add our new fields for numeric values and units
    new_fields = fieldnames + [
        'latest_funding_in_millions', 'latest_funding_original_unit', 
        'valuation_in_millions', 'valuation_original_unit'
    ]

    writer = csv.DictWriter(outfile, fieldnames=new_fields)
    writer.writeheader()

    for row in reader:
        # Clean company_size
        cs = row.get('company_size', '')
        if cs:
            numeric_size = parse_company_size(cs)
        else:
            numeric_size = None

        # founded_year to int
        fy = row.get('founded_year', '')
        if fy:
            fy_val = parse_founded_year(fy)
        else:
            fy_val = None

        # valuation
        val_str = row.get('valuation', '')
        val_numeric, val_unit = parse_currency_field(val_str)

        # latest_funding
        lf_str = row.get('latest_funding', '')
        lf_numeric, lf_unit = parse_currency_field(lf_str)

        # Overwrite fields or leave as is?
        # We'll leave original fields as is and add new columns
        if numeric_size is not None:
            row['company_size'] = numeric_size
        else:
            row['company_size'] = ''

        if fy_val is not None:
            row['founded_year'] = fy_val
        else:
            row['founded_year'] = ''

        row['latest_funding_in_millions'] = lf_numeric if lf_numeric is not None else ''
        row['latest_funding_original_unit'] = lf_unit if lf_unit else ''

        row['valuation_in_millions'] = val_numeric if val_numeric is not None else ''
        row['valuation_original_unit'] = val_unit if val_unit else ''

        writer.writerow(row)

print(f"Cleaned data with units written to {output_file}")
