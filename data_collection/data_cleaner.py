"""
数据清洗工具
去重、格式规范化、无效数据过滤、岗位有效性校验
"""

import json
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
from loguru import logger
import pandas as pd
from urllib.parse import urlparse


class DataCleaner:
    """数据清洗器"""
    
    def __init__(self):
        self.seen_hashes: Set[str] = set()
        self.valid_educations = {
            "不限", "专科", "本科", "硕士", "博士",
            "大专", "学士", "研究生", "PhD"
        }
        
    def clean_jobs(self, jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        清洗岗位数据
        
        Args:
            jobs: 原始岗位列表
            
        Returns:
            清洗后的岗位列表
        """
        logger.info(f"Starting to clean {len(jobs)} jobs")
        
        cleaned_jobs = []
        stats = {
            "total": len(jobs),
            "duplicates": 0,
            "invalid_url": 0,
            "missing_fields": 0,
            "expired": 0,
            "valid": 0
        }
        
        for job in jobs:
            # 去重检查
            if self._is_duplicate(job):
                stats["duplicates"] += 1
                continue
                
            # 字段完整性检查
            if not self._check_required_fields(job):
                stats["missing_fields"] += 1
                continue
                
            # URL 有效性检查
            if not self._is_valid_url(job.get("apply_url", "")):
                stats["invalid_url"] += 1
                continue
                
            # 截止日期检查
            if self._is_expired(job.get("deadline", "")):
                stats["expired"] += 1
                continue
                
            # 规范化字段
            cleaned_job = self._normalize_job(job)
            cleaned_jobs.append(cleaned_job)
            stats["valid"] += 1
            
        logger.info(f"Cleaning complete: {stats}")
        return cleaned_jobs
    
    def _is_duplicate(self, job: Dict[str, Any]) -> bool:
        """检查是否重复"""
        # 使用公司名+岗位名+地点生成哈希
        key = f"{job.get('company_name', '')}{job.get('job_title', '')}{job.get('location', '')}"
        job_hash = hashlib.md5(key.encode()).hexdigest()
        
        if job_hash in self.seen_hashes:
            return True
            
        self.seen_hashes.add(job_hash)
        return False
    
    def _check_required_fields(self, job: Dict[str, Any]) -> bool:
        """检查必填字段"""
        required_fields = ["company_name", "job_title", "apply_url"]
        
        for field in required_fields:
            value = job.get(field, "")
            if not value or not str(value).strip():
                logger.debug(f"Missing required field: {field}")
                return False
                
        return True
    
    def _is_valid_url(self, url: str) -> bool:
        """验证URL有效性"""
        try:
            result = urlparse(url)
            return all([result.scheme in ["http", "https"], result.netloc])
        except Exception:
            return False
    
    def _is_expired(self, deadline: str) -> bool:
        """检查是否过期"""
        if not deadline:
            return False
            
        try:
            # 尝试多种日期格式
            date_formats = [
                "%Y-%m-%d",
                "%Y/%m/%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S",
            ]
            
            deadline_date = None
            for fmt in date_formats:
                try:
                    deadline_date = datetime.strptime(deadline, fmt)
                    break
                except ValueError:
                    continue
                    
            if deadline_date:
                return deadline_date < datetime.now()
                
        except Exception as e:
            logger.debug(f"Failed to parse deadline: {deadline}, {e}")
            
        return False
    
    def _normalize_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """规范化岗位数据"""
        normalized = job.copy()
        
        # 清理文本字段
        text_fields = ["company_name", "job_title", "location", "description", "requirements"]
        for field in text_fields:
            if field in normalized:
                normalized[field] = self._clean_text(normalized[field])
        
        # 规范化学历
        if "education" in normalized:
            normalized["education"] = self._normalize_education(normalized["education"])
        
        # 规范化地点
        if "location" in normalized:
            normalized["location"] = self._normalize_location(normalized["location"])
        
        # 添加清洗时间戳
        normalized["cleaned_time"] = datetime.now().isoformat()
        
        return normalized
    
    def _clean_text(self, text: str) -> str:
        """清理文本"""
        if not text:
            return ""
            
        # 去除多余空白
        text = " ".join(str(text).split())
        
        # 去除特殊字符
        text = text.replace("\xa0", " ")
        text = text.replace("\u3000", " ")
        text = text.replace("\t", " ")
        
        return text.strip()
    
    def _normalize_education(self, education: str) -> str:
        """规范化学历"""
        education = self._clean_text(education)
        
        # 映射到标准学历
        edu_mapping = {
            "大专": "专科",
            "专科及以上": "专科",
            "本科及以上": "本科",
            "硕士及以上": "硕士",
            "博士及以上": "博士",
            "学士": "本科",
            "研究生": "硕士",
            "PhD": "博士",
        }
        
        for key, value in edu_mapping.items():
            if key in education:
                return value
                
        return education if education in self.valid_educations else "不限"
    
    def _normalize_location(self, location: str) -> str:
        """规范化地点"""
        location = self._clean_text(location)
        
        # 提取主要城市名
        import re
        
        # 常见城市列表
        major_cities = [
            "北京", "上海", "广州", "深圳", "杭州", "南京", "成都", "武汉",
            "西安", "重庆", "天津", "苏州", "长沙", "郑州", "青岛", "大连"
        ]
        
        for city in major_cities:
            if city in location:
                return city
                
        return location
    
    def deduplicate_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        使用 pandas 去重
        
        Args:
            df: 原始 DataFrame
            
        Returns:
            去重后的 DataFrame
        """
        logger.info(f"Deduplicating {len(df)} rows")
        
        # 基于关键字段去重
        df_dedup = df.drop_duplicates(
            subset=["company_name", "job_title", "location"],
            keep="first"
        )
        
        logger.info(f"After deduplication: {len(df_dedup)} rows")
        return df_dedup
    
    def save_cleaned_data(self, jobs: List[Dict[str, Any]], output_path: str):
        """保存清洗后的数据"""
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(jobs, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(jobs)} cleaned jobs to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save cleaned data: {e}")


def main():
    """测试数据清洗"""
    # 加载原始数据
    raw_jobs = []
    
    # 示例数据
    sample_jobs = [
        {
            "company_name": "字节跳动",
            "job_title": "后端开发工程师",
            "location": "北京市海淀区",
            "education": "本科及以上",
            "apply_url": "https://jobs.bytedance.com/12345",
            "deadline": "2025-12-31",
        },
        {
            "company_name": "字节跳动",  # 重复
            "job_title": "后端开发工程师",
            "location": "北京市海淀区",
            "education": "本科",
            "apply_url": "https://jobs.bytedance.com/12345",
        },
        {
            "company_name": "腾讯",
            "job_title": "算法工程师",
            "location": "深圳",
            "education": "硕士",
            "apply_url": "invalid_url",  # 无效URL
        },
    ]
    
    cleaner = DataCleaner()
    cleaned = cleaner.clean_jobs(sample_jobs)
    
    print(f"Cleaned {len(cleaned)} jobs from {len(sample_jobs)} original jobs")
    print(json.dumps(cleaned, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

