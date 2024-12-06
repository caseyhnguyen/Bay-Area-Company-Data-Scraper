from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
import time

def setup_driver():
    """Set up Chrome driver with necessary options"""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    return webdriver.Chrome(options=options)

def get_page_content(driver, page_number):
    """Load a specific page and return its content"""
    url = f"https://topstartups.io/?page={page_number}&hq_location=San+Francisco+Bay+Area"
    driver.get(url)
    time.sleep(3)  # Wait for page to load
    return driver.page_source

def extract_startup_info(card):
    """Extract information from a single startup card"""
    startup = {}
    
    try:
        startup['name'] = card.find('h3').text.strip()
    except:
        startup['name'] = None

    try:
        description_element = card.find('b', text='What they do: ')
        startup['description'] = description_element.find_next_sibling(text=True).strip() if description_element else None
    except:
        startup['description'] = None
    
    try:
        industry_tags = card.find_all('span', {'class': 'badge rounded-pill bg-success'})
        startup['industries'] = [tag.text.strip() for tag in industry_tags if tag.get('id') == 'industry-tags']
    except:
        startup['industries'] = []

    try:
        quick_facts = card.find('b', text='Quick facts: ')
        location_text = quick_facts.find_next_sibling(text=True)
        startup['location'] = location_text.strip().split('📍HQ: ')[1].split('\n')[0] if location_text else None
    except:
        startup['location'] = None

    try:
        size_tags = card.find_all('span', {'id': 'company-size-tags'})
        startup['company_size'] = size_tags[0].text.strip() if size_tags else None
        startup['founded_year'] = size_tags[1].text.strip().replace('Founded: ', '') if len(size_tags) > 1 else None
    except:
        startup['company_size'] = None
        startup['founded_year'] = None

    try:
        funding_tags = card.find_all('span', {'id': 'funding-tags'})
        startup['investors'] = []
        startup['latest_funding'] = None
        startup['valuation'] = None
        
        for tag in funding_tags:
            text = tag.text.strip()
            if 'Series' in text or 'seed' in text.lower():
                startup['latest_funding'] = text
            elif 'valuation' in text.lower():
                startup['valuation'] = text
            else:
                startup['investors'].append(text)
    except:
        startup['investors'] = []
        startup['latest_funding'] = None
        startup['valuation'] = None

    # Add website URL if available
    try:
        website_link = card.find('a', {'id': 'startup-website-link'})
        startup['website'] = website_link['href'].split('?')[0] if website_link else None
    except:
        startup['website'] = None

    return startup

def scrape_startups():
    """Main function to scrape all startup data"""
    driver = setup_driver()
    startups = []
    page_number = 1
    total_processed = 0
    
    try:
        print("Starting scraping process...")
        
        while True:
            print(f"Loading page {page_number}...")
            page_content = get_page_content(driver, page_number)
            soup = BeautifulSoup(page_content, 'html.parser')
            
            # Find all startup cards on the current page
            startup_cards = soup.find_all('div', {'class': 'card card-body', 'id': 'item-card-filter'})
            
            # Check if we've reached the end (no startup cards found)
            if not startup_cards:
                print("No more startups found.")
                break
            
            # Process startup cards
            for card in startup_cards:
                startup_info = extract_startup_info(card)
                if startup_info['name']:  # Only add if we found a name
                    startups.append(startup_info)
                    total_processed += 1
                    if total_processed % 10 == 0:
                        print(f"Processed {total_processed} startups")
            
            # Check if there's a "Show more" link
            if not soup.find('a', {'class': 'infinite-more-link'}):
                print("Reached the last page.")
                break
                
            page_number += 1
            time.sleep(1)  # Brief pause between pages
        
        print(f"\nTotal startups processed: {total_processed}")
        
        # Convert to DataFrame and save
        df = pd.DataFrame(startups)
        
        # Clean and format the data
        df['investors'] = df['investors'].apply(lambda x: ', '.join(x) if x else '')
        df['industries'] = df['industries'].apply(lambda x: ', '.join(x) if x else '')
        
        # Save to CSV
        filename = 'sf_bay_area_startups.csv'
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\nSuccessfully saved {len(startups)} startups to {filename}")
        
        # Print some basic statistics
        print("\nQuick Statistics:")
        print(f"Total number of startups: {len(df)}")
        print(f"Number of unique industries: {len(set(','.join(df['industries']).split(', ')))}")
        print(f"Most common company size: {df['company_size'].mode().iloc[0]}")
        print(f"Year range: {df['founded_year'].min()} - {df['founded_year'].max()}")
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
        
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_startups()