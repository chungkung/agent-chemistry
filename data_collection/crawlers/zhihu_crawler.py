"""
知乎爬虫
爬取求职建议、简历优化相关高赞回答
"""

import json
import re
from datetime import datetime
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from loguru import logger

from .utils import CrawlerUtils, clean_text


class ZhihuCrawler:
    """知乎求职内容爬虫"""
    
    BASE_URL = "https://www.zhihu.com"
    API_URL = "https://www.zhihu.com/api/v4"
    
    def __init__(self):
        self.utils = CrawlerUtils(request_interval=(3, 6), max_retries=3)
        self.collected_content = []
        
    def crawl_topic_answers(
        self,
        topic_id: str,
        limit: int = 100,
        sort_by: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        爬取话题下的回答
        
        Args:
            topic_id: 话题 ID (例如: 19550825 - 求职)
            limit: 爬取数量限制
            sort_by: 排序方式 (default, updated)
            
        Returns:
            回答列表
        """
        logger.info(f"Crawling Zhihu topic {topic_id}, limit: {limit}")
        
        all_answers = []
        offset = 0
        page_size = 20
        
        while len(all_answers) < limit:
            url = f"{self.API_URL}/topics/{topic_id}/feeds/essence"
            params = {
                "include": "data[*].target.content,excerpt,author,voteup_count",
                "limit": page_size,
                "offset": offset,
                "sort_by": sort_by
            }
            
            response = self.utils.safe_request(url, params=params)
            if not response:
                break
                
            data = self.utils.parse_json(response)
            if not data or "data" not in data:
                break
                
            items = data["data"]
            if not items:
                break
                
            for item in items:
                answer_info = self._parse_answer(item)
                if answer_info:
                    all_answers.append(answer_info)
                    
            offset += page_size
            
            if not data.get("paging", {}).get("is_end", True):
                break
                
        logger.info(f"Crawled {len(all_answers)} answers from Zhihu topic {topic_id}")
        self.collected_content.extend(all_answers)
        return all_answers
    
    def crawl_search_results(
        self,
        query: str,
        search_type: str = "content",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        搜索知乎内容
        
        Args:
            query: 搜索关键词
            search_type: 搜索类型 (content, question, answer)
            limit: 结果数量限制
            
        Returns:
            搜索结果列表
        """
        logger.info(f"Searching Zhihu for: {query}")
        
        all_results = []
        offset = 0
        page_size = 20
        
        while len(all_results) < limit:
            url = f"{self.API_URL}/search_v3"
            params = {
                "q": query,
                "t": search_type,
                "offset": offset,
                "limit": page_size,
            }
            
            response = self.utils.safe_request(url, params=params)
            if not response:
                break
                
            data = self.utils.parse_json(response)
            if not data or "data" not in data:
                break
                
            items = data["data"]
            if not items:
                break
                
            for item in items:
                result = self._parse_search_result(item, query)
                if result:
                    all_results.append(result)
                    
            offset += page_size
            
        logger.info(f"Found {len(all_results)} search results for: {query}")
        self.collected_content.extend(all_results)
        return all_results
    
    def _parse_answer(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """解析回答信息"""
        try:
            target = item.get("target", {})
            author = target.get("author", {})
            
            # 提取纯文本内容
            content_html = target.get("content", "")
            soup = BeautifulSoup(content_html, "html.parser")
            content_text = soup.get_text(separator="\n", strip=True)
            
            answer_info = {
                "source": "zhihu",
                "type": "answer",
                "answer_id": str(target.get("id", "")),
                "question_title": target.get("question", {}).get("title", ""),
                "author_name": author.get("name", ""),
                "content": clean_text(content_text),
                "excerpt": clean_text(target.get("excerpt", "")),
                "voteup_count": target.get("voteup_count", 0),
                "comment_count": target.get("comment_count", 0),
                "url": f"https://www.zhihu.com/question/{target.get('question', {}).get('id', '')}/answer/{target.get('id', '')}",
                "created_time": target.get("created_time", ""),
                "updated_time": target.get("updated_time", ""),
                "crawl_time": datetime.now().isoformat(),
            }
            
            return answer_info
            
        except Exception as e:
            logger.error(f"Failed to parse answer: {e}")
            return None
    
    def _parse_search_result(self, item: Dict[str, Any], query: str) -> Dict[str, Any]:
        """解析搜索结果"""
        try:
            obj = item.get("object", {})
            obj_type = item.get("type", "")
            
            # 提取文本内容
            content = ""
            if "excerpt" in obj:
                content = clean_text(obj["excerpt"])
            elif "content" in obj:
                soup = BeautifulSoup(obj["content"], "html.parser")
                content = soup.get_text(separator="\n", strip=True)
            
            result = {
                "source": "zhihu",
                "type": obj_type,
                "query": query,
                "title": clean_text(obj.get("title", "")),
                "content": content,
                "author": obj.get("author", {}).get("name", ""),
                "voteup_count": obj.get("voteup_count", 0),
                "url": obj.get("url", ""),
                "crawl_time": datetime.now().isoformat(),
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to parse search result: {e}")
            return None
    
    def extract_job_advice(self, content: str) -> Dict[str, List[str]]:
        """
        从内容中提取求职建议
        
        Args:
            content: 文本内容
            
        Returns:
            分类后的建议字典
        """
        advice = {
            "resume": [],
            "interview": [],
            "strategy": [],
            "skills": []
        }
        
        # 简单的关键词匹配提取
        lines = content.split("\n")
        for line in lines:
            line = line.strip()
            if not line or len(line) < 10:
                continue
                
            if any(kw in line for kw in ["简历", "CV", "resume"]):
                advice["resume"].append(line)
            elif any(kw in line for kw in ["面试", "interview", "笔试"]):
                advice["interview"].append(line)
            elif any(kw in line for kw in ["策略", "技巧", "方法"]):
                advice["strategy"].append(line)
            elif any(kw in line for kw in ["技能", "能力", "项目"]):
                advice["skills"].append(line)
                
        return advice
    
    def save_to_json(self, filepath: str):
        """保存结果到 JSON 文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(self.collected_content, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.collected_content)} items to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save content: {e}")


def main():
    """测试爬虫"""
    crawler = ZhihuCrawler()
    
    # 搜索求职相关内容
    queries = [
        "简历优化建议",
        "互联网求职攻略",
        "大厂面试经验",
        "应届生求职",
    ]
    
    for query in queries:
        results = crawler.crawl_search_results(query, limit=10)
        print(f"Found {len(results)} results for: {query}")
        
    # 保存结果
    crawler.save_to_json("zhihu_content.json")


if __name__ == "__main__":
    main()

