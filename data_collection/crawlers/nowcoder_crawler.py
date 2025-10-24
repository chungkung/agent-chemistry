"""
牛客网爬虫
爬取校招、实习岗位信息
"""

import json
import time
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from loguru import logger

from .utils import CrawlerUtils, clean_text, validate_url


class NowcoderCrawler:
    """牛客网招聘信息爬虫"""
    
    BASE_URL = "https://www.nowcoder.com"
    JOBS_API = "https://www.nowcoder.com/api/sparta/pc/campus/job/query"
    
    def __init__(self):
        self.utils = CrawlerUtils(request_interval=(3, 6), max_retries=3)
        self.collected_jobs = []
        
    def crawl_campus_jobs(
        self,
        job_type: str = "all",
        page_limit: int = 50,
        keywords: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        爬取校招岗位
        
        Args:
            job_type: 岗位类型 (all, tech, product, design, operation)
            page_limit: 爬取页数限制
            keywords: 关键词过滤
            
        Returns:
            岗位列表
        """
        logger.info(f"Starting to crawl Nowcoder campus jobs, type: {job_type}")
        
        all_jobs = []
        page = 1
        
        while page <= page_limit:
            logger.info(f"Crawling page {page}/{page_limit}")
            
            # 构建请求参数
            params = {
                "pageSize": 20,
                "pageNum": page,
                "order": "newest",
                "jobType": job_type if job_type != "all" else ""
            }
            
            response = self.utils.safe_request(
                self.JOBS_API,
                method="POST",
                json=params,
                headers=self.utils.get_headers({
                    "Referer": "https://www.nowcoder.com/school/schedule",
                    "Content-Type": "application/json"
                })
            )
            
            if not response:
                logger.warning(f"Failed to fetch page {page}")
                break
                
            data = self.utils.parse_json(response)
            if not data or data.get("code") != 0:
                logger.warning(f"Invalid response at page {page}")
                break
                
            jobs = data.get("data", {}).get("jobs", [])
            if not jobs:
                logger.info("No more jobs found")
                break
                
            # 解析岗位信息
            for job_item in jobs:
                job_info = self._parse_job_item(job_item)
                if job_info and self._filter_job(job_info, keywords):
                    all_jobs.append(job_info)
                    
            page += 1
            
        logger.info(f"Crawled {len(all_jobs)} jobs from Nowcoder")
        self.collected_jobs.extend(all_jobs)
        return all_jobs
    
    def _parse_job_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """解析单个岗位信息"""
        try:
            job_info = {
                "source": "nowcoder",
                "job_id": str(item.get("jobId", "")),
                "company_name": clean_text(item.get("corpName", "")),
                "job_title": clean_text(item.get("jobName", "")),
                "job_type": clean_text(item.get("jobType", "")),
                "location": clean_text(item.get("cityName", "")),
                "education": clean_text(item.get("education", "")),
                "salary": clean_text(item.get("salary", "")),
                "description": clean_text(item.get("jobDesc", "")),
                "requirements": clean_text(item.get("jobRequire", "")),
                "apply_url": f"https://www.nowcoder.com/school/schedule/{item.get('jobId', '')}",
                "deadline": item.get("endTime", ""),
                "publish_time": item.get("publishTime", ""),
                "tags": item.get("tags", []),
                "crawl_time": datetime.now().isoformat(),
            }
            
            # 验证 URL
            if not validate_url(job_info["apply_url"]):
                logger.warning(f"Invalid URL for job: {job_info['job_title']}")
                return None
                
            return job_info
            
        except Exception as e:
            logger.error(f"Failed to parse job item: {e}")
            return None
    
    def _filter_job(self, job: Dict[str, Any], keywords: List[str] = None) -> bool:
        """根据关键词过滤岗位"""
        if not keywords:
            return True
            
        text = f"{job['job_title']} {job['description']} {job['requirements']}".lower()
        return any(keyword.lower() in text for keyword in keywords)
    
    def save_to_json(self, filepath: str):
        """保存结果到 JSON 文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.collected_jobs, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.collected_jobs)} jobs to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save jobs: {e}")


def main():
    """测试爬虫"""
    crawler = NowcoderCrawler()
    
    # 爬取互联网技术岗位
    jobs = crawler.crawl_campus_jobs(
        job_type="tech",
        page_limit=5,
        keywords=["后端", "前端", "算法", "Java", "Python"]
    )
    
    print(f"Collected {len(jobs)} jobs")
    
    # 保存结果
    crawler.save_to_json("nowcoder_jobs.json")


if __name__ == "__main__":
    main()

