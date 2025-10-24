"""
生成测试用的模拟数据
用于快速原型开发和测试
"""

import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
from loguru import logger


class MockDataGenerator:
    """模拟数据生成器"""
    
    def __init__(self):
        self.companies = [
            # 互联网
            "字节跳动", "腾讯", "阿里巴巴", "百度", "美团",
            "京东", "网易", "小米", "快手", "拼多多",
            # 外企
            "微软", "谷歌", "亚马逊", "苹果", "Meta",
            # 金融
            "中信证券", "华泰证券", "招商银行", "工商银行",
            # 制造业
            "华为", "比亚迪", "宁德时代", "海尔",
        ]
        
        self.positions = {
            "技术": [
                "后端开发工程师", "前端开发工程师", "算法工程师", 
                "数据工程师", "测试工程师", "运维工程师",
                "移动开发工程师", "全栈工程师", "架构师"
            ],
            "产品": [
                "产品经理", "产品运营", "数据产品经理",
                "用户研究", "交互设计师", "UI设计师"
            ],
            "市场": [
                "市场营销", "品牌推广", "商务拓展",
                "销售经理", "渠道经理"
            ],
            "职能": [
                "人力资源", "财务分析", "法务专员",
                "行政助理", "项目经理"
            ]
        }
        
        self.locations = [
            "北京", "上海", "深圳", "杭州", "广州",
            "成都", "南京", "武汉", "西安", "苏州"
        ]
        
        self.educations = ["本科", "硕士", "博士", "不限"]
        
        self.majors = [
            "计算机科学", "软件工程", "人工智能", "数据科学",
            "电子工程", "自动化", "通信工程", "机械工程",
            "金融学", "会计学", "工商管理", "市场营销",
            "新闻传播", "英语", "法学", "数学"
        ]
    
    def generate_jobs(self, num: int = 500) -> List[Dict[str, Any]]:
        """
        生成模拟岗位数据
        
        Args:
            num: 生成数量
            
        Returns:
            岗位列表
        """
        logger.info(f"Generating {num} mock jobs")
        
        jobs = []
        
        for i in range(num):
            category = random.choice(list(self.positions.keys()))
            position = random.choice(self.positions[category])
            company = random.choice(self.companies)
            location = random.choice(self.locations)
            education = random.choice(self.educations)
            
            # 生成薪资范围
            if "工程师" in position or "经理" in position:
                salary_low = random.randint(15, 30)
                salary_high = salary_low + random.randint(10, 20)
                salary = f"{salary_low}-{salary_high}K"
            else:
                salary_low = random.randint(8, 20)
                salary_high = salary_low + random.randint(5, 15)
                salary = f"{salary_low}-{salary_high}K"
            
            # 生成截止日期（未来1-3个月）
            deadline_days = random.randint(30, 90)
            deadline = (datetime.now() + timedelta(days=deadline_days)).strftime("%Y-%m-%d")
            
            job = {
                "source": "mock",
                "job_id": f"mock_{i:04d}",
                "company_name": company,
                "job_title": position,
                "category": category,
                "location": location,
                "education": education,
                "salary": salary,
                "description": self._generate_description(position, category),
                "requirements": self._generate_requirements(position, education),
                "tags": self._generate_tags(position, category),
                "apply_url": f"https://jobs.example.com/position/{i}",
                "deadline": deadline,
                "publish_time": (datetime.now() - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
                "crawl_time": datetime.now().isoformat(),
            }
            
            jobs.append(job)
        
        logger.info(f"Generated {len(jobs)} mock jobs")
        return jobs
    
    def _generate_description(self, position: str, category: str) -> str:
        """生成岗位描述"""
        descriptions = {
            "技术": [
                "负责公司核心业务系统的设计和开发",
                "参与技术架构设计和代码评审",
                "优化系统性能，提升用户体验",
                "与产品、设计团队协作完成需求开发",
            ],
            "产品": [
                "负责产品规划和需求分析",
                "设计产品原型和交互流程",
                "跟进开发进度，确保产品质量",
                "分析用户反馈，持续优化产品",
            ],
            "市场": [
                "制定市场推广策略",
                "开拓新客户，维护客户关系",
                "组织品牌活动，提升品牌影响力",
                "分析市场趋势，提供决策支持",
            ],
            "职能": [
                "负责日常行政事务",
                "协助团队完成项目管理",
                "处理财务、人事等相关工作",
                "支持业务部门的运营需求",
            ]
        }
        
        return "\n".join(random.sample(descriptions.get(category, descriptions["职能"]), 3))
    
    def _generate_requirements(self, position: str, education: str) -> str:
        """生成任职要求"""
        base_requirements = [
            f"{education}及以上学历，相关专业优先",
            "具备良好的沟通能力和团队协作精神",
            "责任心强，能承受工作压力",
        ]
        
        if "开发" in position or "工程师" in position:
            base_requirements.extend([
                "熟练掌握至少一门编程语言（Java/Python/Go/C++）",
                "了解常用的数据结构和算法",
                "有实际项目经验者优先",
            ])
        elif "产品" in position:
            base_requirements.extend([
                "熟悉产品设计流程和方法",
                "掌握Axure、Figma等产品工具",
                "有互联网产品经验者优先",
            ])
        elif "算法" in position:
            base_requirements.extend([
                "熟悉机器学习、深度学习相关算法",
                "掌握TensorFlow、PyTorch等框架",
                "有论文发表或竞赛获奖经历优先",
            ])
        
        return "\n".join(base_requirements)
    
    def _generate_tags(self, position: str, category: str) -> List[str]:
        """生成标签"""
        all_tags = ["五险一金", "弹性工作", "带薪年假", "团建活动"]
        
        if "工程师" in position:
            all_tags.extend(["技术培训", "Mac办公", "扁平管理"])
        
        return random.sample(all_tags, min(4, len(all_tags)))
    
    def generate_user_queries(self, num: int = 100) -> List[str]:
        """
        生成用户查询样本
        
        Args:
            num: 生成数量
            
        Returns:
            查询列表
        """
        logger.info(f"Generating {num} mock user queries")
        
        queries = []
        
        templates = [
            "{education}{major}，想找{location}的{company_type}{position}",
            "应届{education}，{major}背景，目标{position}，{location}优先",
            "求职{position}，{education}学历，专业{major}，希望在{location}",
            "{major}专业{education}毕业生，寻找{company_type}的{position}机会",
            "本人{major}{education}，想去{company_type}做{position}，地点{location}",
        ]
        
        company_types = [
            "互联网大厂", "外企", "国企", "创业公司", "金融机构"
        ]
        
        for i in range(num):
            template = random.choice(templates)
            query = template.format(
                education=random.choice(self.educations),
                major=random.choice(self.majors),
                location=random.choice(self.locations),
                company_type=random.choice(company_types),
                position=random.choice([p for positions in self.positions.values() for p in positions])
            )
            queries.append(query)
        
        return queries
    
    def save_to_json(self, data: List[Any], filepath: str):
        """保存数据到JSON文件"""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(data)} items to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")


def main():
    """测试模拟数据生成"""
    generator = MockDataGenerator()
    
    # 生成岗位数据
    jobs = generator.generate_jobs(num=500)
    generator.save_to_json(jobs, "mock_jobs.json")
    
    # 生成用户查询
    queries = generator.generate_user_queries(num=100)
    generator.save_to_json(queries, "mock_queries.json")
    
    print(f"Generated {len(jobs)} mock jobs and {len(queries)} mock queries")


if __name__ == "__main__":
    main()

