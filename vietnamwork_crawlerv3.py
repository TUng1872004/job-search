import csv
import json
import time
import random
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

from search import HybridJDCVMatching
from kafka_service import KafkaService
from elasticsearch import Elasticsearch
from openai import OpenAI

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
WINDOW_SIZE = "1920,1080"
VIETNAMWORK_GMAIL = os.getenv("VNW_GMAIL")
VIETNAMWORK_PASSWORD = os.getenv("VNW_PASSWORD")

# Number of parallel browser instances. 
# Warning: Each worker opens a Chrome window. Adjust based on available RAM (e.g., 3-4 workers for 8GB RAM).
MAX_WORKERS = 4  

# Thread locks to ensure safe writing to shared resources (files/lists) in a multi-threaded environment
file_lock = Lock()
data_lock = Lock()

class VietNamWorkWebCrawler:

    def __init__(self, kafka_service: KafkaService):
        self.kafka = kafka_service
        self.list_jobs = []
        # Initialize the main driver (Visible UI) for navigating the job list and pagination
        self.main_driver = self.init_driver(headless=False) 

    def init_driver(self, headless=False):
        """
        Initializes a Chrome WebDriver with specific configurations.
        Args:
            headless (bool): If True, runs without a UI window (used for worker threads to save resources).
        """
        chrome_options = Options()
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--incognito")  # Run in Incognito mode
        
        # Anti-detection settings to prevent websites from blocking the bot
        chrome_options.add_argument("--disable-blink-features=AutomationControllered")
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')

        # Disable notifications and password saving prompts
        prefs = {
        "profile.default_content_setting_values.notifications": 2, 
        "credentials_enable_service": False, 
        "profile.password_manager_enabled": False} 
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Worker threads run headless for performance; Main thread runs visibly for monitoring
        if headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
        else:
            chrome_options.add_argument("--start-maximized")

        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver

    @staticmethod
    def scroll_and_wait(driver, last_height):
        """
        Scrolls down the page to trigger lazy loading of content.
        """
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        time.sleep(1)

    @staticmethod
    def random_delay(min_s, max_s):
        """Adds a random delay to mimic human behavior."""
        time.sleep(random.uniform(min_s, max_s))

    def get_num_pages(self):
        """
        Calculates the total number of pages based on the total job count text.
        """
        driver = self.main_driver
        driver.get("https://www.vietnamworks.com/tim-viec-lam/tim-tat-ca-viec-lam")
        self.random_delay(2, 4)
        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '(//span[@class="no-of-jobs"])[2]'))
            )
            text = element.text
            number = int(text.replace(",", "").strip())
            page = int(number/50) + (1 if number % 50 != 0 else 0)
            return page
        except:
            return 1 # Default to 1 page if detection fails

    # --- WORKER FUNCTION: FULLY ISOLATED ---
    def process_job_detail(self, link_url):
        """
        This function runs inside a separate thread.
        It manages its own Lifecycle: Init Driver -> Scrape -> Quit Driver.
        CRITICAL: Do not use shared class variables here to avoid race conditions.
        """
        worker_driver = None
        job_data = None
        
        # Get thread name for debugging purposes
        t_name = threading.current_thread().name
        
        try:
            # 1. Initialize a private driver for this thread
            worker_driver = self.init_driver(headless=True)
            
            # Print session ID to verify different browser instances are running
            print(f"[{t_name} | ID:{worker_driver.session_id[:6]}] >> Start: {link_url}")
            
            worker_driver.get(link_url)
            time.sleep(2) # Wait for page load

            last_height = worker_driver.execute_script("return document.body.scrollHeight")
            self.scroll_and_wait(worker_driver, last_height)

            # 2. Scraping Logic (Using local variables)
            # ------------------------------------------------
            try:
                ten_cv = worker_driver.find_element(By.XPATH, '//h1[@name="title"]').text
            except Exception as e:
                ten_cv = '' 
                print(f"Error extracting title: {e}")

            self.random_delay(1, 3)

            try:
                diadiem_cv = worker_driver.find_element(By.XPATH, '(.//*[normalize-space(text()) and normalize-space(.)=\'Địa điểm làm việc\'])[1]/following::div[3]').text
            except: diadiem_cv = ''
            
            try:
                # Click "View full description" if the button exists
                btns = worker_driver.find_elements(By.XPATH, '//button[@aria-label="Xem đầy đủ mô tả công việc"]')
                if btns:
                    worker_driver.execute_script("arguments[0].click();", btns[0])
                    time.sleep(1)
                mota_cv = worker_driver.find_element(By.XPATH, '(.//*[normalize-space(text()) and normalize-space(.)=\'Nộp đơn\'])[1]/following::div[5]').text
            except: mota_cv = ''

            try:
                kynang_cv = worker_driver.find_element(By.XPATH, '(.//*[normalize-space(text()) and normalize-space(.)=\'KỸ NĂNG\'])[1]/following::p[1]').text
            except: kynang_cv = ''

            try:
                so_nam_kn_min = worker_driver.find_element(By.XPATH, '(.//*[normalize-space(text()) and normalize-space(.)=\'SỐ NĂM KINH NGHIỆM TỐI THIỂU\'])[1]/following::p[1]').text
            except: so_nam_kn_min = 0

            # 3. Package data into a dictionary
            job_data = {
                "title": ten_cv,
                "location": diadiem_cv,
                "description": mota_cv,
                "required_skills": kynang_cv,
                "min_experience_years": so_nam_kn_min,
                "link": link_url
            }
            # ------------------------------------------------

        except Exception as e:
            print(f"[{t_name}] Exception: {e}")
        finally:
            # 4. CRITICAL: Always quit the driver to release RAM/CPU
            if worker_driver:
                worker_driver.quit()
        
        return job_data
    
    @staticmethod
    def clean_location(text):
        """Removes metadata like 'Expired' or 'Views' from location text."""
        if not isinstance(text, str): return ""
        lines = text.split('\n')
        clean_lines = [line.strip() for line in lines if "Hết hạn" not in line and "lượt xem" not in line]
        return ", ".join(clean_lines)

    @staticmethod
    def clean_description(text):
        """Standardizes the description text (removes headers, fixes newlines)."""
        if not isinstance(text, str): return ""
        text = re.sub(r'(?i)mô tả công việc[:\s]*', '', text)
        text = re.sub(r'\n+', '\n', text).strip()
        return text

    def get_job_links(self):
        """Retrieves all job block elements from the current list page."""
        try:
            # Wait for job blocks to appear
            all_blocks = WebDriverWait(self.main_driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '//div[@class="block-job-list"]/child::div'))
            )
            return all_blocks
        except Exception as e:
            print(f"Error getting job blocks: {e}")
            return []

    def run_crawler(self, start_page=1, end_page=5):
        """
        Main Loop:
        1. Navigate pages using Main Driver.
        2. Collect Job Links.
        3. Dispatch Links to Worker Threads.
        4. Send results to Kafka.
        """
        
        # Load history of crawled links to prevent duplicates (Incremental Crawling)
        crawled_history = set()
        if os.path.exists('links.csv'):
            with open('links.csv', 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row: 
                        # Clean URL (remove query params) for comparison
                        clean_h = row[0].split('?')[0].split('#')[0].rstrip('/')
                        crawled_history.add(clean_h)

        try:
            for i in range(start_page, end_page + 1):
                print(f"\n--- PROCESSING PAGE {i}/{end_page} ---")
                self.main_driver.get(f"https://www.vietnamworks.com/viec-lam?page={i}")
                
                last_height = self.main_driver.execute_script("return document.body.scrollHeight")
                self.scroll_and_wait(self.main_driver, last_height)
                
                # --- LINK FILTERING LOGIC ---
                # Find all anchor tags that look like job details ('-jv')
                job_elements = self.main_driver.find_elements(By.XPATH, '//div[@class="block-job-list"]//a[contains(@href, "-jv")]')
                
                # Use a Set to avoid processing the same link twice on one page
                unique_urls_in_page = set()

                for elem in job_elements:
                    try:
                        raw_url = elem.get_attribute('href')
                        if not raw_url: continue
                        
                        if "/nha-tuyen-dung/" in raw_url: continue # Skip company profile links
                        
                        # Clean URL before checking history
                        clean_link = raw_url.split('?')[0].split('#')[0].rstrip('/')
                        
                        if clean_link not in crawled_history:
                            unique_urls_in_page.add(raw_url) # Add original URL for processing
                            crawled_history.add(clean_link)  # Add clean URL to history
                            
                            # Append to history file immediately
                            with open('links.csv', 'a', newline='') as f:
                                csv.writer(f).writerow([raw_url])
                    except:
                        pass
                
                batch_urls = list(unique_urls_in_page)

                if batch_urls:
                    print(f"-> Found {len(batch_urls)} NEW JOBS. Dispatching to {MAX_WORKERS} threads...")
                    
                    # Initialize ThreadPoolExecutor
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        # Map futures to URLs
                        future_to_url = {executor.submit(self.process_job_detail, url): url for url in batch_urls}
                        
                        for future in as_completed(future_to_url):
                            result = future.result()
                            if result:
                                    # Send successful scrape result to Kafka
                                    self.kafka.produce_message('job_postings', key=None, value=json.dumps(result))
                                    print(f"[OK] {result['title']}")
                else:
                    print("No new jobs found on this page.")

        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            if self.main_driver:
                self.main_driver.quit()

if __name__ == "__main__":
    # Initialize Infrastructure Services
    kafka = KafkaService(bootstrap_servers='localhost:9092')
    es_client = Elasticsearch("http://localhost:9200")
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    # Setup Elasticsearch Index (Custom class from 'search' module)
    matcher = HybridJDCVMatching(es_client=es_client, openai_client=openai_client)
    matcher.create_index("job_postings")
    
    # Start the Crawler
    crawler = VietNamWorkWebCrawler(kafka_service=kafka)
    crawler.run_crawler(start_page=1, end_page=1)