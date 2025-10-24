"""
构建符合 Mistral 格式的微调数据集
包含三种任务：用户输入解析、岗位匹配、建议生成
"""

import json
import random
from typing import List, Dict, Any
from loguru import logger


class DatasetBuilder:
    """微调数据集构建器"""
    
    MISTRAL_PROMPT_TEMPLATE = "<s>[INST] {instruction} [/INST] {response}</s>"
    
    def __init__(self):
        self.train_data = []
        self.eval_data = []
        self.test_data = []
        
    def build_from_annotations(
        self,
        annotations: List[Dict[str, Any]],
        train_ratio: float = 0.8,
        eval_ratio: float = 0.1
    ):
        """
        从标注数据构建训练集
        
        Args:
            annotations: 标注数据列表
            train_ratio: 训练集比例
            eval_ratio: 验证集比例
        """
        logger.info(f"Building dataset from {len(annotations)} annotations")
        
        # 按类型分组
        grouped_data = {
            "user_input_parsing": [],
            "job_matching": [],
            "advice_generation": [],
        }
        
        for ann in annotations:
            ann_type = ann.get("type")
            if ann_type in grouped_data:
                grouped_data[ann_type].append(ann)
        
        # 为每种类型构建数据
        all_samples = []
        
        for ann_type, items in grouped_data.items():
            logger.info(f"Processing {len(items)} {ann_type} samples")
            
            if ann_type == "user_input_parsing":
                samples = self._build_input_parsing_samples(items)
            elif ann_type == "job_matching":
                samples = self._build_job_matching_samples(items)
            elif ann_type == "advice_generation":
                samples = self._build_advice_samples(items)
            else:
                continue
                
            all_samples.extend(samples)
        
        # 随机打乱
        random.shuffle(all_samples)
        
        # 划分数据集
        total = len(all_samples)
        train_size = int(total * train_ratio)
        eval_size = int(total * eval_ratio)
        
        self.train_data = all_samples[:train_size]
        self.eval_data = all_samples[train_size:train_size + eval_size]
        self.test_data = all_samples[train_size + eval_size:]
        
        logger.info(f"Dataset split: Train={len(self.train_data)}, "
                   f"Eval={len(self.eval_data)}, Test={len(self.test_data)}")
    
    def _build_input_parsing_samples(
        self,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """构建用户输入解析样本"""
        samples = []
        
        for item in items:
            user_input = item["user_input"]
            params = item["structured_params"]
            
            # 构建指令
            instruction = f"""你是一个招聘助手，负责解析用户的求职需求。
请根据用户输入，提取关键信息并以 JSON 格式输出，包括：专业(major)、学历(education)、目标企业类型(target_company_type)、地点(location)、岗位类型(job_type)、职位(position)。

用户输入：{user_input}"""
            
            # 构建响应
            response = json.dumps(params, ensure_ascii=False, indent=2)
            
            # 使用 Mistral 格式
            text = self.MISTRAL_PROMPT_TEMPLATE.format(
                instruction=instruction,
                response=response
            )
            
            samples.append({"text": text})
        
        return samples
    
    def _build_job_matching_samples(
        self,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """构建岗位匹配样本"""
        samples = []
        
        for item in items:
            user_params = item["user_params"]
            jobs = item["jobs"]
            
            # 构建岗位列表文本
            jobs_text = ""
            for i, job_item in enumerate(jobs, 1):
                job = job_item["job"]
                jobs_text += f"{i}. {job.get('company_name', '')} - {job.get('job_title', '')} ({job.get('location', '')})\n"
            
            # 构建指令
            instruction = f"""你是一个招聘助手，负责为用户匹配合适的岗位。
根据用户需求和岗位列表，选出最匹配的前5个岗位，并说明推荐理由。

用户需求：
- 专业：{user_params.get('major', '')}
- 学历：{user_params.get('education', '')}
- 目标企业：{user_params.get('target_company_type', '')}
- 地点：{user_params.get('location', '')}

候选岗位：
{jobs_text}

请输出匹配结果（JSON格式），包含岗位序号、匹配度分数(0-1)、推荐理由。"""
            
            # 构建响应（取前5个高匹配度岗位）
            top_jobs = sorted(jobs, key=lambda x: x["match_score"], reverse=True)[:5]
            response_data = []
            for i, job_item in enumerate(top_jobs, 1):
                job = job_item["job"]
                response_data.append({
                    "rank": i,
                    "company": job.get("company_name", ""),
                    "position": job.get("job_title", ""),
                    "location": job.get("location", ""),
                    "match_score": job_item["match_score"],
                    "reason": job_item["reason"],
                })
            
            response = json.dumps(response_data, ensure_ascii=False, indent=2)
            
            text = self.MISTRAL_PROMPT_TEMPLATE.format(
                instruction=instruction,
                response=response
            )
            
            samples.append({"text": text})
        
        return samples
    
    def _build_advice_samples(
        self,
        items: List[Dict[str, Any]]
    ) -> List[Dict[str, str]]:
        """构建建议生成样本"""
        samples = []
        
        for item in items:
            scenario = item["scenario"]
            advice = item["advice"]
            
            # 构建指令
            instruction = f"""你是一个资深的职业规划顾问，请根据求职场景提供针对性的建议。

求职场景：{scenario}

请提供以下三方面的建议（每方面3-5条）：
1. 简历优化建议
2. 面试准备建议
3. 投递策略建议"""
            
            # 构建响应
            response = f"""## 简历优化建议

{self._format_tips_list(advice.get('resume_optimization', []))}

## 面试准备建议

{self._format_tips_list(advice.get('interview_preparation', []))}

## 投递策略建议

{self._format_tips_list(advice.get('application_strategy', []))}"""
            
            text = self.MISTRAL_PROMPT_TEMPLATE.format(
                instruction=instruction,
                response=response
            )
            
            samples.append({"text": text})
        
        return samples
    
    def _format_tips_list(self, tips: List[str]) -> str:
        """格式化建议列表"""
        return "\n".join([f"- {tip}" for tip in tips])
    
    def save_datasets(
        self,
        train_path: str = "./data/train.jsonl",
        eval_path: str = "./data/eval.jsonl",
        test_path: str = "./data/test.jsonl"
    ):
        """保存数据集到文件"""
        self._save_jsonl(self.train_data, train_path)
        self._save_jsonl(self.eval_data, eval_path)
        self._save_jsonl(self.test_data, test_path)
        
        logger.info(f"Saved datasets: train={len(self.train_data)}, "
                   f"eval={len(self.eval_data)}, test={len(self.test_data)}")
    
    def _save_jsonl(self, data: List[Dict], filepath: str):
        """保存为 JSONL 格式"""
        try:
            import os
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
            logger.info(f"Saved {len(data)} samples to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save {filepath}: {e}")
    
    def get_dataset_stats(self) -> Dict[str, Any]:
        """获取数据集统计信息"""
        return {
            "train_size": len(self.train_data),
            "eval_size": len(self.eval_data),
            "test_size": len(self.test_data),
            "total_size": len(self.train_data) + len(self.eval_data) + len(self.test_data),
        }


def main():
    """测试数据集构建"""
    # 加载标注数据
    try:
        with open("annotations.json", "r", encoding="utf-8") as f:
            annotations = json.load(f)
    except FileNotFoundError:
        logger.warning("annotations.json not found, using mock data")
        annotations = [
            {
                "type": "user_input_parsing",
                "user_input": "计算机本科应届生，想找北京的互联网大厂后端开发岗位",
                "structured_params": {
                    "major": "计算机",
                    "education": "本科",
                    "target_company_type": "互联网大厂",
                    "location": "北京",
                    "job_type": "全职",
                    "position": "后端开发",
                }
            }
        ]
    
    builder = DatasetBuilder()
    builder.build_from_annotations(annotations)
    builder.save_datasets()
    
    stats = builder.get_dataset_stats()
    print(f"Dataset statistics: {stats}")


if __name__ == "__main__":
    main()

