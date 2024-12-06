import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from dataclasses import dataclass
from typing import List, Optional, Dict
import logging
import time
from urllib.parse import urljoin

@dataclass
class CompanyData:
    name: str
    industry: str
    headquarters: str
    type: Optional[str] = None
    founded: Optional[str] = None
    revenue: Optional[str] = None
    employees: Optional[int] = None
    services: Optional[List[str]] = None
    subsidiaries: Optional[List[str]] = None
    traded_as: Optional[List[str]] = None
    formerly: Optional[str] = None
    founders: Optional[List[str]] = None
    key_people: Optional[List[str]] = None
    website: Optional[str] = None
    wikipedia_url: Optional[str] = None
    description: Optional[str] = None

class WikiCompanyScraper:
    def __init__(self):
        self.companies = []
        self.base_url = "https://en.wikipedia.org"
        self.session = requests.Session()
        self.setup_logging()
        
        # Set user agent for Wikipedia
        self.session.headers.update({
            'User-Agent': 'BayAreaCompanyScraper/1.0 (Research Project)'
        })

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('company_scraper.log'),
                logging.StreamHandler()
            ]
        )

    def get_page(self, url: str, delay: float = 1.0) -> Optional[BeautifulSoup]:
        """Get a Wikipedia page with rate limiting"""
        try:
            time.sleep(delay)  # Rate limiting
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            return None

    def parse_list_entry(self, text: str, industry: str) -> Optional[Dict]:
        """Parse a company entry from the list format"""
        try:
            # Split on dash/hyphen characters
            parts = re.split(r'\s+[–-]\s+', text, maxsplit=1)
            if len(parts) != 2:
                logging.warning(f"Could not parse company entry: {text}")
                return None

            name, location = parts
            name = name.strip()
            location = location.strip()

            # Extract any notes in parentheses
            notes = None
            if '(' in location:
                location_match = re.match(r'(.*?)\s*(?:\((.*?)\))?$', location)
                if location_match:
                    location = location_match.group(1).strip()
                    notes = location_match.group(2)

            # Look for Fortune 500 rank
            fortune_rank = None
            rank_match = re.search(r'\((\d+)\)', name)
            if rank_match:
                fortune_rank = int(rank_match.group(1))
                name = re.sub(r'\s*\(\d+\)', '', name)

            return {
                'name': name,
                'industry': industry,
                'headquarters': location,
                'fortune_500_rank': fortune_rank,
                'notes': notes
            }
        except Exception as e:
            logging.error(f"Error parsing list entry '{text}': {str(e)}")
            return None

    def parse_company_list(self, content: str) -> List[Dict]:
        """Parse the initial company list page"""
        soup = BeautifulSoup(content, 'html.parser')
        current_industry = None
        companies = []
        
        for element in soup.find_all(['h2', 'h3', 'ul']):
            if element.name in ['h2', 'h3']:
                # Get text within span if it exists, otherwise use full text
                industry_elem = element.find('span', {'class': 'mw-headline'})
                if industry_elem:
                    current_industry = industry_elem.get_text().strip()
                else:
                    current_industry = element.get_text().strip()
                
            elif element.name == 'ul' and current_industry:
                for item in element.find_all('li'):
                    link = item.find('a', href=True)
                    companies.append({
                        'industry': current_industry,
                        'text': item.get_text().strip(),
                        'link': link['href'] if link else None
                    })
        
        return companies

    def parse_infobox(self, soup: BeautifulSoup) -> Dict:
        """Parse company infobox data"""
        infobox = soup.find('table', {'class': 'infobox'})
        if not infobox:
            return {}
            
        data = {}
        
        # Map infobox fields to data fields
        field_mapping = {
            'Industry': 'industry',
            'Founded': 'founded',
            'Headquarters': 'headquarters',
            'Revenue': 'revenue',
            'Number of employees': 'employees',
            'Type': 'type',
            'Traded as': 'traded_as',
            'Founders': 'founders',
            'Key people': 'key_people',
            'Services': 'services',
            'Subsidiaries': 'subsidiaries',
            'Website': 'website'
        }

        for row in infobox.find_all('tr'):
            header = row.find('th')
            if not header:
                continue
                
            header_text = header.get_text().strip()
            if header_text in field_mapping:
                value = row.find('td')
                if value:
                    field_name = field_mapping[header_text]
                    
                    # Handle list values
                    if field_name in ['services', 'subsidiaries', 'founders', 'key_people']:
                        items = value.find_all('li')
                        if items:
                            data[field_name] = [item.get_text().strip() for item in items]
                        else:
                            data[field_name] = [v.strip() for v in value.get_text().split(',')]
                    else:
                        data[field_name] = value.get_text().strip()

        return data

    def process_companies(self, content: str):
        """Process company list and scrape individual pages"""
        companies = self.parse_company_list(content)
        
        for company in companies:
            logging.info(f"Processing {company['text']}")
            
            # Get basic info from list
            basic_data = self.parse_list_entry(company['text'], company['industry'])
            if not basic_data:
                continue

            # If company has a Wikipedia link, get detailed info
            if company['link'] and company['link'].startswith('/wiki/'):
                url = urljoin(self.base_url, company['link'])
                detailed_page = self.get_page(url)
                if detailed_page:
                    detailed_data = self.parse_infobox(detailed_page)
                    basic_data.update(detailed_data)
                    basic_data['wikipedia_url'] = url

            self.companies.append(basic_data)

    def save_to_csv(self, filename='bay_area_companies.csv'):
        """Save company data to CSV with extended statistics"""
        if not self.companies:
            logging.error("No company data to save!")
            return

        df = pd.DataFrame(self.companies)
        df.to_csv(filename, index=False)
        
        logging.info(f"\nSaved {len(df)} companies to {filename}")
        
        # Generate statistics
        logging.info("\n=== Dataset Statistics ===")
        logging.info(f"Total Companies: {len(df)}")
        
        if 'industry' in df.columns:
            logging.info("\nTop Industries:")
            logging.info(df['industry'].value_counts().head())
        
        if 'employees' in df.columns:
            valid_employees = pd.to_numeric(df['employees'], errors='coerce')
            logging.info(f"\nTotal Employees: {valid_employees.sum():,.0f}")
            logging.info(f"Average Employees: {valid_employees.mean():,.0f}")
        
        if 'fortune_500_rank' in df.columns:
            logging.info(f"\nFortune 500 Companies: {df['fortune_500_rank'].notna().sum()}")

def main():
    scraper = WikiCompanyScraper()
    
    # Start with Bay Area companies list
    list_url = "https://en.wikipedia.org/wiki/List_of_companies_based_in_the_San_Francisco_Bay_Area"
    content = scraper.get_page(list_url)
    
    if content:
        scraper.process_companies(str(content))
        scraper.save_to_csv()

if __name__ == "__main__":
    main()

# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# from dataclasses import dataclass
# from typing import List, Optional, Dict
# import logging
# import time
# from urllib.parse import urljoin
# from datetime import datetime

# @dataclass
# class CompanyData:
#     name: str
#     industry: str
#     headquarters: str
#     type: Optional[str] = None
#     founded: Optional[str] = None
#     revenue: Optional[str] = None
#     employees: Optional[int] = None
#     services: Optional[List[str]] = None
#     subsidiaries: Optional[List[str]] = None
#     traded_as: Optional[List[str]] = None
#     formerly: Optional[str] = None
#     founders: Optional[List[str]] = None
#     key_people: Optional[List[str]] = None
#     website: Optional[str] = None
#     wikipedia_url: Optional[str] = None
#     description: Optional[str] = None

# class WikiCompanyScraper:
#     def __init__(self):
#         self.companies = []
#         self.base_url = "https://en.wikipedia.org"
#         self.session = requests.Session()
#         self.setup_logging()
        
#         # Set user agent for Wikipedia
#         self.session.headers.update({
#             'User-Agent': 'BayAreaCompanyScraper/1.0 (Research Project)'
#         })

#     def setup_logging(self):
#         logging.basicConfig(
#             level=logging.INFO,
#             format='%(asctime)s - %(levelname)s - %(message)s',
#             handlers=[
#                 logging.FileHandler('company_scraper.log'),
#                 logging.StreamHandler()
#             ]
#         )

#     def get_page(self, url: str, delay: float = 1.0) -> Optional[BeautifulSoup]:
#         """Get a Wikipedia page with rate limiting"""
#         try:
#             time.sleep(delay)  # Rate limiting
#             response = self.session.get(url)
#             response.raise_for_status()
#             return BeautifulSoup(response.text, 'html.parser')
#         except Exception as e:
#             logging.error(f"Error fetching {url}: {str(e)}")
#             return None

#     def parse_list_entry(self, text: str, industry: str) -> Optional[Dict]:
#         """Parse a company entry from the list format"""
#         try:
#             # Split on dash/hyphen characters
#             parts = re.split(r'\s+[–-]\s+', text, maxsplit=1)
#             if len(parts) != 2:
#                 logging.warning(f"Could not parse company entry: {text}")
#                 return None

#             name, location = parts
#             name = name.strip()
#             location = location.strip()

#             # Extract any notes in parentheses
#             notes = None
#             if '(' in location:
#                 location_match = re.match(r'(.*?)\s*(?:\((.*?)\))?$', location)
#                 if location_match:
#                     location = location_match.group(1).strip()
#                     notes = location_match.group(2)

#             # Look for Fortune 500 rank
#             fortune_rank = None
#             rank_match = re.search(r'\((\d+)\)', name)
#             if rank_match:
#                 fortune_rank = int(rank_match.group(1))
#                 name = re.sub(r'\s*\(\d+\)', '', name)

#             return {
#                 'name': name,
#                 'industry': industry,
#                 'headquarters': location,
#                 'fortune_500_rank': fortune_rank,
#                 'notes': notes
#             }
#         except Exception as e:
#             logging.error(f"Error parsing list entry '{text}': {str(e)}")
#             return None

#     def parse_company_list(self, content: str) -> List[Dict]:
#         """Parse the initial company list page"""
#         soup = BeautifulSoup(content, 'html.parser')
#         current_industry = None
#         companies = []
        
#         for element in soup.find_all(['h2', 'h3', 'ul']):
#             if element.name in ['h2', 'h3']:
#                 # Get text within span if it exists, otherwise use full text
#                 industry_elem = element.find('span', {'class': 'mw-headline'})
#                 if industry_elem:
#                     current_industry = industry_elem.get_text().strip()
#                 else:
#                     current_industry = element.get_text().strip()
                
#             elif element.name == 'ul' and current_industry:
#                 for item in element.find_all('li'):
#                     link = item.find('a', href=True)
#                     companies.append({
#                         'industry': current_industry,
#                         'text': item.get_text().strip(),
#                         'link': link['href'] if link else None
#                     })
        
#         return companies

#     def parse_infobox(self, soup: BeautifulSoup) -> Dict:
#         """Parse company infobox data"""
#         infobox = soup.find('table', {'class': 'infobox'})
#         if not infobox:
#             return {}
            
#         data = {}
        
#         # Map infobox fields to data fields
#         field_mapping = {
#             'Industry': 'industry',
#             'Founded': 'founded',
#             'Headquarters': 'headquarters',
#             'Revenue': 'revenue',
#             'Number of employees': 'employees',
#             'Type': 'type',
#             'Traded as': 'traded_as',
#             'Founders': 'founders',
#             'Key people': 'key_people',
#             'Services': 'services',
#             'Subsidiaries': 'subsidiaries',
#             'Website': 'website'
#         }

#         for row in infobox.find_all('tr'):
#             header = row.find('th')
#             if not header:
#                 continue
                
#             header_text = header.get_text().strip()
#             if header_text in field_mapping:
#                 value = row.find('td')
#                 if value:
#                     field_name = field_mapping[header_text]
                    
#                     # Handle list values
#                     if field_name in ['services', 'subsidiaries', 'founders', 'key_people']:
#                         items = value.find_all('li')
#                         if items:
#                             data[field_name] = [item.get_text().strip() for item in items]
#                         else:
#                             data[field_name] = [v.strip() for v in value.get_text().split(',')]
#                     else:
#                         data[field_name] = value.get_text().strip()

#         return data

#     def process_companies(self, content: str):
#         """Process company list and scrape individual pages"""
#         companies = self.parse_company_list(content)
        
#         for company in companies:
#             logging.info(f"Processing {company['text']}")
            
#             # Get basic info from list
#             basic_data = self.parse_list_entry(company['text'], company['industry'])
#             if not basic_data:
#                 continue

#             # If company has a Wikipedia link, get detailed info
#             if company['link'] and company['link'].startswith('/wiki/'):
#                 url = urljoin(self.base_url, company['link'])
#                 detailed_page = self.get_page(url)
#                 if detailed_page:
#                     detailed_data = self.parse_infobox(detailed_page)
#                     basic_data.update(detailed_data)
#                     basic_data['wikipedia_url'] = url

#             self.companies.append(basic_data)

#     def save_to_csv(self, filename='bay_area_companies.csv'):
#         """Save company data to CSV with all requested fields and startup logic"""

#         if not self.companies:
#             logging.error("No company data to save!")
#             return

#         current_year = datetime.now().year
#         processed_rows = []

#         for c in self.companies:
#             # Original fields
#             name = c.get('name', '')
#             industry = c.get('industry', '')
#             headquarters = c.get('headquarters', '')
#             fortune_500_rank = c.get('fortune_500_rank', '')
#             notes = c.get('notes', '')
#             traded_as = c.get('traded_as', '')
#             founded = c.get('founded', '')
#             revenue = c.get('revenue', '')
#             employees_str = c.get('employees', '')
#             website = c.get('website', '')
#             wikipedia_url = c.get('wikipedia_url', '')
#             subsidiaries = c.get('subsidiaries', '')
#             founders = c.get('founders', '')
#             services = c.get('services', '')
#             comp_type = c.get('type', '')

#             # New fields: description, industries, company_size, founded_year, investors, latest_funding, valuation, startup
#             # We don't have description or extra industries from Wiki, so keep blank or reuse industry as industries
#             description = ''  # Could be populated with additional scraping if desired
#             industries_field = industry  # Using the same industry as 'industries'

#             # Parse employees into company_size
#             company_size = ''
#             emp_count = None
#             if employees_str:
#                 try:
#                     emp_count = int(re.sub(r'[^\d]', '', employees_str))
#                     if emp_count < 11:
#                         company_size = '1-10 employees'
#                     elif emp_count < 51:
#                         company_size = '11-50 employees'
#                     elif emp_count < 201:
#                         company_size = '51-200 employees'
#                     elif emp_count < 501:
#                         company_size = '201-500 employees'
#                     elif emp_count < 1001:
#                         company_size = '501-1000 employees'
#                     else:
#                         company_size = '1001-5000 employees'
#                 except:
#                     emp_count = None

#             # Extract founded_year
#             founded_year = ''
#             if founded:
#                 match = re.search(r'(\d{4})', founded)
#                 if match:
#                     founded_year = match.group(1)

#             investors = ''      # Not available from Wikipedia
#             latest_funding = '' # Not available from Wikipedia
#             valuation = ''      # Not available from Wikipedia

#             # Determine startup
#             # Let's say startup = founded < 10 years ago and emp_count < 500
#             startup = 'No'
#             if founded_year:
#                 try:
#                     fyear = int(founded_year)
#                     age = current_year - fyear
#                     if age < 10 and emp_count is not None and emp_count < 500:
#                         startup = 'Yes'
#                 except:
#                     pass

#             row = {
#                 'name': name,
#                 'industry': industry,
#                 'headquarters': headquarters,
#                 'fortune_500_rank': fortune_500_rank,
#                 'notes': notes,
#                 'traded_as': traded_as,
#                 'founded': founded,
#                 'revenue': revenue,
#                 'employees': employees_str,
#                 'website': website,
#                 'wikipedia_url': wikipedia_url,
#                 'subsidiaries': ', '.join(subsidiaries) if isinstance(subsidiaries, list) else subsidiaries,
#                 'founders': ', '.join(founders) if isinstance(founders, list) else founders,
#                 'services': ', '.join(services) if isinstance(services, list) else services,
#                 'type': comp_type,
#                 'description': description,
#                 'industries': industries_field,
#                 'company_size': company_size,
#                 'founded_year': founded_year,
#                 'investors': investors,
#                 'latest_funding': latest_funding,
#                 'valuation': valuation,
#                 'startup': startup
#             }
#             processed_rows.append(row)

#         # Define columns
#         fieldnames = [
#             'name', 'industry', 'headquarters', 'fortune_500_rank', 'notes', 'traded_as', 'founded', 'revenue',
#             'employees', 'website', 'wikipedia_url', 'subsidiaries', 'founders', 'services', 'type',
#             'description', 'industries', 'company_size', 'founded_year', 'investors', 'latest_funding', 'valuation', 'startup'
#         ]

#         df = pd.DataFrame(processed_rows, columns=fieldnames)
#         df.to_csv(filename, index=False, encoding='utf-8')

#         logging.info(f"\nSaved {len(df)} companies to {filename}")
        
#         # Generate statistics
#         logging.info("\n=== Dataset Statistics ===")
#         logging.info(f"Total Companies: {len(df)}")
        
#         if 'industry' in df.columns:
#             logging.info("\nTop Industries:")
#             logging.info(df['industry'].value_counts().head())
        
#         if 'employees' in df.columns:
#             valid_employees = pd.to_numeric(df['employees'], errors='coerce')
#             logging.info(f"\nTotal Employees: {valid_employees.sum():,.0f}")
#             logging.info(f"Average Employees: {valid_employees.mean():,.0f}")
        
#         if 'fortune_500_rank' in df.columns:
#             logging.info(f"\nFortune 500 Companies: {df['fortune_500_rank'].notna().sum()}")

# def main():
#     scraper = WikiCompanyScraper()
    
#     # Start with Bay Area companies list
#     list_url = "https://en.wikipedia.org/wiki/List_of_companies_based_in_the_San_Francisco_Bay_Area"
#     content = scraper.get_page(list_url)
    
#     if content:
#         scraper.process_companies(str(content))
#         scraper.save_to_csv()

# if __name__ == "__main__":
#     main()
