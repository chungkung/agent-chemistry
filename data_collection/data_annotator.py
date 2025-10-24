"""
数据标注工具
标注用户输入、岗位匹配、求职建议等数据
"""

import json
import random
from typing import List, Dict, Any, Tuple
from loguru import logger


class DataAnnotator:
    """数据标注器"""
    
    def __init__(self):
        self.annotations = []
        
    def annotate_user_input_samples(
        self,
        num_samples: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        生成用户输入 -> 结构化参数的标注样本
        
        Args:
            num_samples: 样本数量
            
        Returns:
            标注样本列表
        """
        logger.info(f"Generating {num_samples} user input annotation samples")
        
        samples = []
        
        # 定义模板和变量
        majors = [
            "计算机科学", "软件工程", "人工智能", "数据科学", "电子工程",
            "机械工程", "金融学", "会计学", "市场营销", "工商管理",
            "新闻传播", "英语", "法学", "医学", "建筑学"
        ]
        
        educations = ["本科", "硕士", "博士", "专科"]
        
        company_types = [
            "互联网大厂", "外企", "国企", "创业公司", "金融机构",
            "咨询公司", "制造业", "教育培训", "医疗健康", "游戏公司"
        ]
        
        locations = [
            "北京", "上海", "深圳", "杭州", "广州", "成都", "南京",
            "武汉", "西安", "苏州", "不限"
        ]
        
        job_types = [
            "全职", "实习", "兼职", "远程", "外派"
        ]
        
        positions = [
            "后端开发", "前端开发", "算法工程师", "数据分析", "产品经理",
            "UI设计师", "运营专员", "市场推广", "财务分析", "人力资源"
        ]
        
        # 生成样本
        for i in range(num_samples):
            # 随机选择参数
            major = random.choice(majors)
            education = random.choice(educations)
            company_type = random.choice(company_types)
            location = random.choice(locations)
            job_type = random.choice(job_types)
            position = random.choice(positions)
            
            # 生成自然语言输入（多种表达方式）
            templates = [
                f"我是{education}{major}专业，想找{company_type}的{position}{job_type}，地点在{location}",
                f"{education}应届生，{major}背景，目标{company_type}，岗位：{position}，{location}优先",
                f"求职意向：{position}，{education}学历，专业{major}，想去{company_type}，{location}地区",
                f"{major}专业{education}毕业生，希望找{location}的{company_type}{position}岗位",
                f"本人{major}{education}，寻找{company_type}在{location}的{position}机会",
            ]
            
            user_input = random.choice(templates)
            
            # 结构化参数
            structured_params = {
                "major": major,
                "education": education,
                "target_company_type": company_type,
                "location": location,
                "job_type": job_type,
                "position": position,
            }
            
            sample = {
                "type": "user_input_parsing",
                "user_input": user_input,
                "structured_params": structured_params,
                "annotation_id": f"input_{i:04d}",
            }
            
            samples.append(sample)
            
        self.annotations.extend(samples)
        logger.info(f"Generated {len(samples)} user input samples")
        return samples
    
    def annotate_job_matching_samples(
        self,
        jobs: List[Dict[str, Any]],
        num_samples: int = 3000
    ) -> List[Dict[str, Any]]:
        """
        生成参数 -> 岗位匹配的标注样本
        
        Args:
            jobs: 岗位数据列表
            num_samples: 样本数量
            
        Returns:
            标注样本列表
        """
        logger.info(f"Generating {num_samples} job matching annotation samples")
        
        samples = []
        
        for i in range(num_samples):
            # 随机选择一个用户查询
            user_params = self._generate_random_user_params()
            
            # 从岗位列表中随机选择若干个岗位
            if len(jobs) > 0:
                sample_jobs = random.sample(jobs, min(10, len(jobs)))
            else:
                # 如果没有真实岗位，生成模拟岗位
                sample_jobs = self._generate_mock_jobs(10)
            
            # 为每个岗位标注匹配度
            annotated_jobs = []
            for job in sample_jobs:
                match_score = self._calculate_match_score(user_params, job)
                reason = self._generate_match_reason(user_params, job, match_score)
                
                annotated_jobs.append({
                    "job": job,
                    "match_score": match_score,
                    "reason": reason,
                })
            
            # 按匹配度排序
            annotated_jobs.sort(key=lambda x: x["match_score"], reverse=True)
            
            sample = {
                "type": "job_matching",
                "user_params": user_params,
                "jobs": annotated_jobs,
                "annotation_id": f"match_{i:04d}",
            }
            
            samples.append(sample)
            
        self.annotations.extend(samples)
        logger.info(f"Generated {len(samples)} job matching samples")
        return samples
    
    def annotate_advice_samples(
        self,
        num_samples: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        生成场景 -> 建议的标注样本
        
        Args:
            num_samples: 样本数量
            
        Returns:
            标注样本列表
        """
        logger.info(f"Generating {num_samples} advice annotation samples")
        
        samples = []
        
        # 定义场景和建议模板
        scenarios = [
            {
                "scenario": "计算机本科应届生求职互联网大厂后端开发",
                "resume_tips": [
                    "突出算法和数据结构项目经验，展示在 LeetCode 或牛客网的刷题成果",
                    "强调参与的开源项目或 GitHub 贡献，体现代码质量",
                    "列出熟悉的技术栈（Java/Python/Go）和框架（Spring Boot/Django）",
                    "简历控制在1页，使用清晰的格式和排版",
                ],
                "interview_tips": [
                    "系统复习计算机基础（操作系统、网络、数据库）",
                    "刷 LeetCode 中等难度题目至少 200 道",
                    "准备常见的系统设计问题（如设计短链接服务）",
                    "了解目标公司的技术栈和业务场景",
                ],
                "strategy_tips": [
                    "提前批投递效率更高，关注公司官网和牛客网动态",
                    "多投递几家公司，保底 offer 很重要",
                    "参加公司举办的技术分享会或线下活动",
                ],
            },
            {
                "scenario": "金融硕士应届生求职投行或咨询公司",
                "resume_tips": [
                    "强调金融建模、估值分析等专业技能",
                    "突出实习经历，尤其是在券商、投行或咨询公司的实习",
                    "展示 CFA、CPA 等相关证书",
                    "列出参与的案例分析比赛或商业竞赛获奖经历",
                ],
                "interview_tips": [
                    "准备常见的估值和财务建模问题",
                    "了解宏观经济形势和金融市场动态",
                    "练习 Case Interview 技巧",
                    "准备英文面试，提升商务英语表达能力",
                ],
                "strategy_tips": [
                    "提前申请暑期实习，转正机会较大",
                    "通过校友内推提高通过率",
                    "关注行业论坛和招聘公众号",
                ],
            },
            {
                "scenario": "社会人士转行互联网产品经理",
                "resume_tips": [
                    "突出之前工作中的产品思维和用户洞察",
                    "展示独立完成的产品设计案例或原型",
                    "列出熟悉的产品工具（Axure、Figma、墨刀）",
                    "强调数据分析能力和用户研究经验",
                ],
                "interview_tips": [
                    "准备产品设计题目（如设计一款XX产品）",
                    "了解常见的产品分析方法（AARRR、用户画像）",
                    "准备竞品分析案例",
                    "展示对行业和目标公司产品的理解",
                ],
                "strategy_tips": [
                    "先从初级产品岗位或产品运营岗位切入",
                    "参加产品经理培训课程，系统学习",
                    "多投递中小型公司或创业公司",
                ],
            },
        ]
        
        # 扩展样本
        for i in range(num_samples):
            scenario_data = random.choice(scenarios)
            
            # 随机抽取建议
            resume_tips = random.sample(scenario_data["resume_tips"], min(3, len(scenario_data["resume_tips"])))
            interview_tips = random.sample(scenario_data["interview_tips"], min(3, len(scenario_data["interview_tips"])))
            strategy_tips = random.sample(scenario_data["strategy_tips"], min(2, len(scenario_data["strategy_tips"])))
            
            sample = {
                "type": "advice_generation",
                "scenario": scenario_data["scenario"],
                "advice": {
                    "resume_optimization": resume_tips,
                    "interview_preparation": interview_tips,
                    "application_strategy": strategy_tips,
                },
                "annotation_id": f"advice_{i:04d}",
            }
            
            samples.append(sample)
            
        self.annotations.extend(samples)
        logger.info(f"Generated {len(samples)} advice samples")
        return samples
    
    def _generate_random_user_params(self) -> Dict[str, Any]:
        """生成随机用户参数"""
        majors = ["计算机", "软件工程", "金融", "市场营销", "会计"]
        educations = ["本科", "硕士", "博士"]
        company_types = ["互联网", "外企", "国企", "创业公司"]
        locations = ["北京", "上海", "深圳", "杭州", "不限"]
        
        return {
            "major": random.choice(majors),
            "education": random.choice(educations),
            "target_company_type": random.choice(company_types),
            "location": random.choice(locations),
        }
    
    def _generate_mock_jobs(self, num: int) -> List[Dict[str, Any]]:
        """生成模拟岗位"""
        companies = ["字节跳动", "腾讯", "阿里巴巴", "美团", "京东"]
        positions = ["后端开发", "前端开发", "算法工程师", "产品经理"]
        locations = ["北京", "上海", "深圳", "杭州"]
        
        jobs = []
        for i in range(num):
            job = {
                "company_name": random.choice(companies),
                "job_title": random.choice(positions),
                "location": random.choice(locations),
                "education": random.choice(["本科", "硕士"]),
            }
            jobs.append(job)
            
        return jobs
    
    def _calculate_match_score(
        self,
        user_params: Dict[str, Any],
        job: Dict[str, Any]
    ) -> float:
        """计算匹配分数"""
        score = 0.5  # 基础分
        
        # 地点匹配
        if user_params.get("location") == job.get("location") or user_params.get("location") == "不限":
            score += 0.2
        
        # 学历匹配（简单判断）
        if user_params.get("education") in job.get("education", ""):
            score += 0.2
        
        # 随机调整
        score += random.uniform(-0.1, 0.1)
        
        return round(min(max(score, 0), 1), 2)
    
    def _generate_match_reason(
        self,
        user_params: Dict[str, Any],
        job: Dict[str, Any],
        score: float
    ) -> str:
        """生成匹配原因"""
        reasons = []
        
        if user_params.get("location") == job.get("location"):
            reasons.append("地点匹配")
        
        if user_params.get("education") in job.get("education", ""):
            reasons.append("学历要求符合")
        
        if score > 0.7:
            reasons.append("高度匹配")
        elif score > 0.5:
            reasons.append("基本匹配")
        else:
            reasons.append("部分匹配")
        
        return "，".join(reasons)
    
    def save_annotations(self, output_path: str):
        """保存标注数据"""
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(self.annotations, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.annotations)} annotations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save annotations: {e}")


def main():
    """测试数据标注"""
    annotator = DataAnnotator()
    
    # 生成各类标注样本
    user_input_samples = annotator.annotate_user_input_samples(num_samples=100)
    job_matching_samples = annotator.annotate_job_matching_samples(jobs=[], num_samples=100)
    advice_samples = annotator.annotate_advice_samples(num_samples=50)
    
    print(f"Generated {len(annotator.annotations)} total annotations")
    
    # 保存
    annotator.save_annotations("annotations.json")


if __name__ == "__main__":
    main()

