import csv
import re

def normalize_location(location_str):
    if not location_str:
        return location_str
    # Normalize spacing and punctuation for U.S.
    # Replace variations like "U.S." or " U.S." with "US"
    normalized = re.sub(r'\bU\.S\.\b', 'US', location_str)
    return normalized.strip()

def clean_headquarters(value):
    if not value:
        return value
    # Remove bracketed citations like [citation needed] and any bracketed text
    cleaned = re.sub(r'\[.*?\]', '', value)
    # Normalize "U.S." to "US"
    cleaned = normalize_location(cleaned)
    return cleaned.strip()

def clean_founded(value):
    if not value:
        return value
    # Remove "X years ago" segments
    cleaned = re.sub(r'\d+\s+years\s+ago\s*\(.*?\)', '', value, flags=re.IGNORECASE)
    cleaned = re.sub(r'\d+\s+years\s+ago', '', cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip('; ')
    return cleaned.strip()

def parse_year_field(value):
    # Extract earliest 4-digit year
    if not value:
        return None
    matches = re.findall(r'(\d{4})', value)
    if matches:
        years = [int(y) for y in matches]
        return min(years)
    return None

def parse_employees(value):
    # Extract numeric part from employees
    if not value:
        return None
    digits = re.findall(r'(\d[\d,\.]*)', value)
    if digits:
        num_str = digits[0].replace(',', '').replace('~','').strip()
        num_str = num_str.rstrip('.')  # Remove trailing dot if any
        if num_str.isdigit():
            return int(num_str)
        else:
            try:
                return int(float(num_str))
            except:
                return None
    return None

def parse_revenue(value):
    # Parse revenue and convert to millions if possible
    if not value or value.strip() == '':
        return None, None

    val_str = value.replace('\xa0',' ').strip()
    # Look for patterns like US$X billion/million
    match = re.search(r'US?\$([\d\.,]+)\s*(billion|million|m|b)?', val_str, re.IGNORECASE)
    if match:
        amount_str = match.group(1).replace(',', '')
        unit_str = match.group(2)
        try:
            amount = float(amount_str)
        except:
            return None, None
        if unit_str:
            unit_str = unit_str.lower()
            if unit_str in ['billion','b']:
                return amount * 1000.0, 'B'
            elif unit_str in ['million','m']:
                return amount, 'M'
        # Default to millions if no unit
        return amount, 'M'
    return None, None

input_file = 'bay_area_companies.csv'
output_file = 'bay_area_companies_cleaned.csv'

with open(input_file, 'r', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    if not fieldnames:
        raise ValueError("No field names found in the input file.")

    # Add new fields
    new_fields = fieldnames + ['founded_year', 'revenue_in_millions', 'revenue_original_unit', 'employees_numeric']

    writer = csv.DictWriter(outfile, fieldnames=new_fields)
    writer.writeheader()

    for row in reader:
        # Clean headquarters
        if 'headquarters' in row:
            row['headquarters'] = clean_headquarters(row['headquarters'])

        # Clean founded and parse year
        founded_str = row.get('founded', '')
        founded_cleaned = clean_founded(founded_str)
        fy = parse_year_field(founded_cleaned)
        row['founded_year'] = fy if fy is not None else ''

        # Parse revenue
        rev_str = row.get('revenue', '')
        rev_millions, rev_unit = parse_revenue(rev_str)
        row['revenue_in_millions'] = rev_millions if rev_millions is not None else ''
        row['revenue_original_unit'] = rev_unit if rev_unit else ''

        # Parse employees
        emp_str = row.get('employees', '')
        emp_num = parse_employees(emp_str)
        row['employees_numeric'] = emp_num if emp_num is not None else ''

        writer.writerow(row)

print(f"Cleaned data saved to {output_file}")
