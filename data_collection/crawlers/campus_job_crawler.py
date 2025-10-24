"""
高校就业网爬虫
爬取公开的校招信息
"""

import json
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from loguru import logger

from .utils import CrawlerUtils, clean_text, validate_url


class CampusJobCrawler:
    """高校就业网爬虫"""
    
    # 常见高校就业网URL（示例）
    CAMPUS_SITES = {
        "ncss": {
            "name": "国家大学生就业服务平台",
            "base_url": "https://www.ncss.cn",
            "jobs_url": "https://www.ncss.cn/student/ajax/search",
        },
        "gaokao_chsi": {
            "name": "学信网就业服务",
            "base_url": "https://job.chsi.com.cn",
            "jobs_url": "https://job.chsi.com.cn/jobs",
        }
    }
    
    def __init__(self):
        self.utils = CrawlerUtils(request_interval=(4, 7), max_retries=3)
        self.collected_jobs = []
        
    def crawl_ncss_jobs(
        self,
        keywords: str = "",
        location: str = "",
        page_limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        爬取国家大学生就业服务平台岗位
        
        Args:
            keywords: 搜索关键词
            location: 地点
            page_limit: 页数限制
            
        Returns:
            岗位列表
        """
        logger.info("Crawling NCSS campus jobs")
        
        all_jobs = []
        page = 1
        
        site_info = self.CAMPUS_SITES["ncss"]
        
        while page <= page_limit:
            logger.info(f"Crawling NCSS page {page}/{page_limit}")
            
            # 构建请求参数
            params = {
                "keyword": keywords,
                "city": location,
                "pageNo": page,
                "pageSize": 20
            }
            
            response = self.utils.safe_request(
                site_info["jobs_url"],
                params=params,
                headers=self.utils.get_headers({
                    "Referer": site_info["base_url"]
                })
            )
            
            if not response:
                logger.warning(f"Failed to fetch NCSS page {page}")
                break
                
            # 尝试解析JSON或HTML
            data = self.utils.parse_json(response)
            if data:
                jobs = self._parse_ncss_json(data)
            else:
                jobs = self._parse_ncss_html(response.text)
                
            if not jobs:
                logger.info("No more jobs found")
                break
                
            all_jobs.extend(jobs)
            page += 1
            
        logger.info(f"Crawled {len(all_jobs)} jobs from NCSS")
        self.collected_jobs.extend(all_jobs)
        return all_jobs
    
    def _parse_ncss_json(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """解析NCSS的JSON响应"""
        jobs = []
        
        try:
            job_list = data.get("data", {}).get("rows", [])
            
            for item in job_list:
                job_info = {
                    "source": "ncss",
                    "job_id": str(item.get("id", "")),
                    "company_name": clean_text(item.get("companyName", "")),
                    "job_title": clean_text(item.get("jobName", "")),
                    "location": clean_text(item.get("city", "")),
                    "education": clean_text(item.get("education", "")),
                    "major": clean_text(item.get("major", "")),
                    "salary": clean_text(item.get("salary", "")),
                    "description": clean_text(item.get("description", "")),
                    "requirements": clean_text(item.get("requirements", "")),
                    "apply_url": item.get("applyUrl", ""),
                    "deadline": item.get("deadline", ""),
                    "publish_time": item.get("publishTime", ""),
                    "crawl_time": datetime.now().isoformat(),
                }
                
                if validate_url(job_info["apply_url"]):
                    jobs.append(job_info)
                    
        except Exception as e:
            logger.error(f"Failed to parse NCSS JSON: {e}")
            
        return jobs
    
    def _parse_ncss_html(self, html: str) -> List[Dict[str, Any]]:
        """解析NCSS的HTML响应"""
        jobs = []
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            job_items = soup.select(".job-item, .position-item")
            
            for item in job_items:
                # 提取基本信息
                title_elem = item.select_one(".job-title, .position-name")
                company_elem = item.select_one(".company-name")
                location_elem = item.select_one(".job-location, .location")
                link_elem = item.select_one("a[href]")
                
                if not title_elem or not link_elem:
                    continue
                    
                job_info = {
                    "source": "ncss",
                    "job_title": clean_text(title_elem.get_text()),
                    "company_name": clean_text(company_elem.get_text()) if company_elem else "",
                    "location": clean_text(location_elem.get_text()) if location_elem else "",
                    "apply_url": link_elem.get("href", ""),
                    "crawl_time": datetime.now().isoformat(),
                }
                
                # 补全URL
                if job_info["apply_url"] and not job_info["apply_url"].startswith("http"):
                    job_info["apply_url"] = f"{self.CAMPUS_SITES['ncss']['base_url']}{job_info['apply_url']}"
                    
                if validate_url(job_info["apply_url"]):
                    jobs.append(job_info)
                    
        except Exception as e:
            logger.error(f"Failed to parse NCSS HTML: {e}")
            
        return jobs
    
    def crawl_university_job_board(
        self,
        university_url: str,
        page_limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        爬取单个高校就业网
        
        Args:
            university_url: 高校就业网URL
            page_limit: 页数限制
            
        Returns:
            岗位列表
        """
        logger.info(f"Crawling university job board: {university_url}")
        
        all_jobs = []
        
        # 检查robots.txt
        if not self.utils.check_robots_txt(university_url):
            logger.warning(f"robots.txt disallows crawling: {university_url}")
            return all_jobs
            
        response = self.utils.safe_request(university_url)
        if not response:
            return all_jobs
            
        # 通用HTML解析
        jobs = self._parse_generic_job_board(response.text, university_url)
        all_jobs.extend(jobs)
        
        logger.info(f"Crawled {len(all_jobs)} jobs from {university_url}")
        self.collected_jobs.extend(all_jobs)
        return all_jobs
    
    def _parse_generic_job_board(self, html: str, base_url: str) -> List[Dict[str, Any]]:
        """通用的就业网HTML解析"""
        jobs = []
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # 尝试多种常见的选择器
            selectors = [
                ".job-list .job-item",
                ".position-list .position-item",
                "ul.jobs li",
                ".recruit-list .recruit-item",
                "table.job-table tr",
            ]
            
            items = []
            for selector in selectors:
                items = soup.select(selector)
                if items:
                    break
                    
            for item in items:
                # 提取链接和标题
                link = item.select_one("a[href]")
                if not link:
                    continue
                    
                title = clean_text(link.get_text())
                url = link.get("href", "")
                
                # 补全相对URL
                if url and not url.startswith("http"):
                    from urllib.parse import urljoin
                    url = urljoin(base_url, url)
                    
                if not validate_url(url):
                    continue
                    
                # 尝试提取公司名
                company_elem = item.select_one(".company, .company-name, .corp")
                company = clean_text(company_elem.get_text()) if company_elem else ""
                
                job_info = {
                    "source": "university",
                    "job_title": title,
                    "company_name": company,
                    "apply_url": url,
                    "crawl_time": datetime.now().isoformat(),
                }
                
                jobs.append(job_info)
                
        except Exception as e:
            logger.error(f"Failed to parse generic job board: {e}")
            
        return jobs
    
    def save_to_json(self, filepath: str):
        """保存结果到JSON文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.collected_jobs, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.collected_jobs)} jobs to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save jobs: {e}")


def main():
    """测试爬虫"""
    crawler = CampusJobCrawler()
    
    # 爬取国家大学生就业服务平台
    jobs = crawler.crawl_ncss_jobs(
        keywords="计算机",
        location="北京",
        page_limit=5
    )
    
    print(f"Collected {len(jobs)} jobs")
    
    # 保存结果
    crawler.save_to_json("campus_jobs.json")


if __name__ == "__main__":
    main()

