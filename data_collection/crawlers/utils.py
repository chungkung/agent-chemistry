"""
爬虫通用工具函数
提供 User-Agent 轮换、请求重试、间隔控制等功能
"""

import time
import random
from typing import Optional, Dict, Any
from fake_useragent import UserAgent
import requests
from loguru import logger


class CrawlerUtils:
    """爬虫工具类"""
    
    def __init__(self, request_interval: tuple = (2, 5), max_retries: int = 3):
        """
        初始化爬虫工具
        
        Args:
            request_interval: 请求间隔范围（秒）
            max_retries: 最大重试次数
        """
        self.request_interval = request_interval
        self.max_retries = max_retries
        self.ua = UserAgent()
        
    def get_random_user_agent(self) -> str:
        """获取随机 User-Agent"""
        try:
            return self.ua.random
        except Exception:
            # 如果 fake_useragent 失败，返回默认 UA
            return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    
    def get_headers(self, extra_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        """
        生成请求头
        
        Args:
            extra_headers: 额外的请求头
            
        Returns:
            完整的请求头字典
        """
        headers = {
            "User-Agent": self.get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        
        if extra_headers:
            headers.update(extra_headers)
            
        return headers
    
    def safe_request(
        self,
        url: str,
        method: str = "GET",
        **kwargs
    ) -> Optional[requests.Response]:
        """
        安全的 HTTP 请求，带重试和间隔控制
        
        Args:
            url: 请求 URL
            method: 请求方法
            **kwargs: 传递给 requests 的其他参数
            
        Returns:
            Response 对象，失败返回 None
        """
        # 添加默认 headers
        if "headers" not in kwargs:
            kwargs["headers"] = self.get_headers()
        
        # 添加超时
        if "timeout" not in kwargs:
            kwargs["timeout"] = 30
            
        for attempt in range(self.max_retries):
            try:
                # 请求间隔控制
                if attempt > 0:
                    delay = random.uniform(*self.request_interval)
                    logger.info(f"Retry {attempt}, waiting {delay:.2f}s...")
                    time.sleep(delay)
                
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                
                # 成功后随机延迟
                time.sleep(random.uniform(*self.request_interval))
                return response
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                
                if attempt == self.max_retries - 1:
                    logger.error(f"All retries failed for URL: {url}")
                    return None
                    
        return None
    
    def parse_json(self, response: requests.Response) -> Optional[Dict[str, Any]]:
        """
        安全地解析 JSON 响应
        
        Args:
            response: Response 对象
            
        Returns:
            解析后的 JSON 数据，失败返回 None
        """
        try:
            return response.json()
        except Exception as e:
            logger.error(f"Failed to parse JSON: {e}")
            return None
    
    def check_robots_txt(self, base_url: str, user_agent: str = "*") -> bool:
        """
        检查 robots.txt 是否允许爬取
        
        Args:
            base_url: 网站基础 URL
            user_agent: User-Agent 字符串
            
        Returns:
            是否允许爬取
        """
        from urllib.robotparser import RobotFileParser
        
        try:
            robots_url = f"{base_url.rstrip('/')}/robots.txt"
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            return rp.can_fetch(user_agent, base_url)
        except Exception as e:
            logger.warning(f"Failed to check robots.txt: {e}")
            # 如果无法检查，默认允许
            return True


def clean_text(text: str) -> str:
    """
    清理文本内容
    
    Args:
        text: 原始文本
        
    Returns:
        清理后的文本
    """
    if not text:
        return ""
    
    # 去除多余空白
    text = " ".join(text.split())
    
    # 去除特殊字符
    text = text.replace("\xa0", " ")
    text = text.replace("\u3000", " ")
    
    return text.strip()


def validate_url(url: str) -> bool:
    """
    验证 URL 格式
    
    Args:
        url: URL 字符串
        
    Returns:
        是否有效
    """
    import validators
    return validators.url(url)


def extract_domain(url: str) -> str:
    """
    提取 URL 的域名
    
    Args:
        url: URL 字符串
        
    Returns:
        域名
    """
    from urllib.parse import urlparse
    return urlparse(url).netloc

