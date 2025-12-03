import csv
import json
import time
import random
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import threading
from search import HybridJDCVMatching
from kafka_service import KafkaService
from elasticsearch import Elasticsearch
from openai import OpenAI

load_dotenv()

# --- CONFIG ---
WINDOW_SIZE = "1920,1080"
VIETNAMWORK_GMAIL = os.getenv("VNW_GMAIL")
VIETNAMWORK_PASSWORD = os.getenv("VNW_PASSWORD")
MAX_WORKERS = 4  # Số lượng trình duyệt chạy song song (Tùy RAM máy, 8GB RAM nên để 3-4)

# Lock dùng để ghi file an toàn khi chạy đa luồng
file_lock = Lock()
data_lock = Lock()

class VietNamWorkWebCrawler:

    def __init__(self, kafka_service: KafkaService):
        self.kafka = kafka_service
        self.list_jobs = []
        # Driver chính dùng để lướt danh sách và phân trang
        self.main_driver = self.init_driver(headless=False) 

    def init_driver(self, headless=False):
        """Hàm khởi tạo driver, hỗ trợ chế độ headless cho worker"""
        chrome_options = Options()
        chrome_options.add_argument("--window-size=%s" % WINDOW_SIZE)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument('disable-infobars')
        chrome_options.add_argument("--incognito")  # Chế độ ẩn danh
        chrome_options.add_argument("--disable-blink-features=AutomationControllered")
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])

        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')

        prefs = {
        "profile.default_content_setting_values.notifications": 2,  # Tắt thông báo
        "credentials_enable_service": False,  # Tắt dịch vụ lưu thông tin đăng nhập
        "profile.password_manager_enabled": False}  # Tắt trình quản lý mật khẩu
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Worker threads nên chạy ẩn (headless) để nhanh và đỡ tốn RAM
        if headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
        else:
            chrome_options.add_argument("--start-maximized")

        driver = webdriver.Chrome(service=Service(), options=chrome_options)
        return driver

    @staticmethod
    def scroll_and_wait(driver, last_height):
        # Logic cuộn trang giữ nguyên, nhưng truyền driver vào
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
        time.sleep(random.uniform(min_s, max_s))

    def get_num_pages(self):
        # Logic login giữ nguyên, dùng self.main_driver
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
            # return page
            return page
        except:
            return 1 # Mặc định 1 trang nếu lỗi

    # --- HÀM WORKER: CÔ LẬP HOÀN TOÀN ---
    def process_job_detail(self, link_url):
        """
        Hàm này chạy trên thread riêng.
        Tự tạo driver -> Cào -> Đóng driver.
        KHÔNG DÙNG CHUNG BẤT CỨ BIẾN NÀO BÊN NGOÀI.
        """
        worker_driver = None
        job_data = None
        
        # Lấy tên luồng để debug
        t_name = threading.current_thread().name
        
        try:
            # 1. Khởi tạo driver riêng (QUAN TRỌNG)
            worker_driver = self.init_driver(headless=True)
            
            # In ra session ID để chứng minh 4 trình duyệt khác nhau
            print(f"[{t_name} | ID:{worker_driver.session_id[:6]}] >> Bắt đầu: {link_url}")
            
            worker_driver.get(link_url)
            time.sleep(2) # Chờ load

            last_height = worker_driver.execute_script("return document.body.scrollHeight")
            self.scroll_and_wait(worker_driver,last_height)

            # 2. Logic cào dữ liệu (Cô lập biến local)
            # ------------------------------------------------
            try:
                ten_cv = worker_driver.find_element(By.XPATH, '//h1[@name="title"]').text
            except Exception as e:
                ten_cv = ''  # Thêm chuỗi rỗng nếu có lỗi
                print(f"Lỗi ở phần lấy tên công việc: {e}")

            self.random_delay(1, 3)

            try:
                diadiem_cv = worker_driver.find_element(By.XPATH, '(.//*[normalize-space(text()) and normalize-space(.)=\'Địa điểm làm việc\'])[1]/following::div[3]').text
            except: diadiem_cv = ''
            
            try:
                # Click xem thêm mô tả
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

            # 3. Đóng gói dữ liệu vào biến local
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
            print(f"[{t_name}] Lỗi ngoại lệ: {e}")
        finally:
            # 4. BẮT BUỘC: Đóng driver riêng sau khi xong
            if worker_driver:
                worker_driver.quit()
        
        return job_data
    
    @staticmethod
    def clean_location(text):
        if not isinstance(text, str): return ""
        lines = text.split('\n')
        clean_lines = [line.strip() for line in lines if "Hết hạn" not in line and "lượt xem" not in line]
        return ", ".join(clean_lines)

    @staticmethod
    def clean_description(text):
        if not isinstance(text, str): return ""
        text = re.sub(r'(?i)mô tả công việc[:\s]*', '', text)
        text = re.sub(r'\n+', '\n', text).strip()
        return text

    def get_job_links(self):
        """
        Dùng lại cách cũ của bạn: Lấy các khối DIV chứa job trước.
        """
        try:
            # Chờ các khối job load xong
            all_blocks = WebDriverWait(self.main_driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '//div[@class="block-job-list"]/child::div'))
            )
            return all_blocks
        except Exception as e:
            print(f"Lỗi khi lấy các block job: {e}")
            return []

    def run_crawler(self, start_page=1, end_page=5):
        # total_pages = self.get_num_pages()
        # if end_page is None or end_page > total_pages:
        #     end_page = total_pages
        
        # print(f"Tổng trang: {total_pages}. Chạy từ trang {start_page} đến {end_page}")

        # Load lịch sử đã cào (CHỈ LẤY LINK SẠCH, KHÔNG LẤY QUERY PARAM)
        crawled_history = set()
        if os.path.exists('links.csv'):
            with open('links.csv', 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row: 
                        # Cắt bỏ ?source=... và #...
                        clean_h = row[0].split('?')[0].split('#')[0].rstrip('/')
                        crawled_history.add(clean_h)

        try:
            for i in range(start_page, end_page + 1):
                print(f"\n--- ĐANG XỬ LÝ TRANG {i}/{end_page} ---")
                self.main_driver.get(f"https://www.vietnamworks.com/viec-lam?page={i}")
                
                last_height = self.main_driver.execute_script("return document.body.scrollHeight")
                self.scroll_and_wait(self.main_driver, last_height)
                
                # --- PHẦN LỌC LINK QUAN TRỌNG ---
                # Lấy tất cả thẻ A có chứa '-jv' (dấu hiệu job)
                job_elements = self.main_driver.find_elements(By.XPATH, '//div[@class="block-job-list"]//a[contains(@href, "-jv")]')
                
                # Dùng Set để loại bỏ trùng lặp ngay trong trang này
                unique_urls_in_page = set()

                for elem in job_elements:
                    try:
                        raw_url = elem.get_attribute('href')
                        if not raw_url: continue
                        
                        if "/nha-tuyen-dung/" in raw_url: continue # Bỏ link cty
                        
                        # LÀM SẠCH URL TRƯỚC KHI CHECK
                        clean_link = raw_url.split('?')[0].split('#')[0].rstrip('/')
                        
                        if clean_link not in crawled_history:
                            unique_urls_in_page.add(raw_url) # Thêm url gốc để chạy
                            crawled_history.add(clean_link)  # Thêm url sạch vào lịch sử
                            
                            # Backup vào file
                            with open('links.csv', 'a', newline='') as f:
                                csv.writer(f).writerow([raw_url])
                    except:
                        pass
                
                # Chuyển set thành list để đưa vào worker
                batch_urls = list(unique_urls_in_page)

                if batch_urls:
                    print(f"-> Tìm thấy {len(batch_urls)} JOB MỚI. Đẩy vào {MAX_WORKERS} luồng...")
                    
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        # Map: {Future: URL}
                        future_to_url = {executor.submit(self.process_job_detail, url): url for url in batch_urls}
                        
                        for future in as_completed(future_to_url):
                            result = future.result()
                            if result:
                                    self.kafka.produce_message('job_postings', key=None, value=json.dumps(result))
                                # with data_lock: # Khóa an toàn khi ghi vào list chung
                                    # self.list_jobs.append(result)
                                    print(f"[OK] {result['title']}")
                else:
                    print("Trang này không có job mới.")
                
                # self.save_data()

        except KeyboardInterrupt:
            print("Đang dừng...")
        finally:
            # self.save_data()
            if self.main_driver:
                self.main_driver.quit()

    # def save_data(self):
    #     with open('job_data.json', 'w', encoding='utf-8') as f:
    #         json.dump(self.list_jobs, f, ensure_ascii=False, indent=4)
    #     print("Đã lưu dữ liệu.")

if __name__ == "__main__":
    kafka = KafkaService(bootstrap_servers='localhost:9092')
    es_client = Elasticsearch("http://localhost:9200")
    openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    matcher = HybridJDCVMatching(es_client=es_client, openai_client=openai_client)
    matcher.create_index("job_postings")

    kafka.create_topic('job_postings', num_partitions=3, replication_factor=1)
    crawler = VietNamWorkWebCrawler(kafka_service=kafka)
    crawler.run_crawler(start_page=1, end_page=1)  # Chạy từ trang 1 đến 1