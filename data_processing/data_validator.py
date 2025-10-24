"""
数据质量校验工具
检查数据完整性、格式正确性、内容合理性
"""

import json
from typing import List, Dict, Any, Tuple
from loguru import logger


class DataValidator:
    """数据质量校验器"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        
    def validate_dataset(
        self,
        dataset_path: str
    ) -> Tuple[bool, List[str], List[str]]:
        """
        校验数据集质量
        
        Args:
            dataset_path: 数据集文件路径
            
        Returns:
            (是否通过, 错误列表, 警告列表)
        """
        logger.info(f"Validating dataset: {dataset_path}")
        
        self.errors = []
        self.warnings = []
        
        try:
            # 加载数据
            with open(dataset_path, "r", encoding="utf-8") as f:
                if dataset_path.endswith(".jsonl"):
                    data = [json.loads(line) for line in f]
                else:
                    data = json.load(f)
                    
            if not data:
                self.errors.append("Dataset is empty")
                return False, self.errors, self.warnings
                
            # 执行各项检查
            self._check_format(data)
            self._check_completeness(data)
            self._check_content_quality(data)
            self._check_statistics(data)
            
            passed = len(self.errors) == 0
            
            logger.info(f"Validation {'passed' if passed else 'failed'}: "
                       f"{len(self.errors)} errors, {len(self.warnings)} warnings")
            
            return passed, self.errors, self.warnings
            
        except Exception as e:
            self.errors.append(f"Failed to load dataset: {e}")
            return False, self.errors, self.warnings
    
    def _check_format(self, data: List[Dict[str, Any]]):
        """检查数据格式"""
        logger.info("Checking data format...")
        
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                self.errors.append(f"Item {i}: Not a dictionary")
                continue
                
            if "text" not in item:
                self.errors.append(f"Item {i}: Missing 'text' field")
                
            if not isinstance(item.get("text"), str):
                self.errors.append(f"Item {i}: 'text' field is not a string")
    
    def _check_completeness(self, data: List[Dict[str, Any]]):
        """检查数据完整性"""
        logger.info("Checking data completeness...")
        
        for i, item in enumerate(data):
            text = item.get("text", "")
            
            # 检查 Mistral 格式
            if not text.startswith("<s>[INST]"):
                self.warnings.append(f"Item {i}: Does not start with Mistral instruction tag")
                
            if "[/INST]" not in text:
                self.errors.append(f"Item {i}: Missing instruction closing tag")
                
            if not text.endswith("</s>"):
                self.warnings.append(f"Item {i}: Does not end with closing tag")
            
            # 检查长度
            if len(text) < 50:
                self.warnings.append(f"Item {i}: Text too short ({len(text)} chars)")
            elif len(text) > 2048:
                self.warnings.append(f"Item {i}: Text very long ({len(text)} chars), may need truncation")
    
    def _check_content_quality(self, data: List[Dict[str, Any]]):
        """检查内容质量"""
        logger.info("Checking content quality...")
        
        # 检查重复
        seen_texts = set()
        duplicates = 0
        
        for i, item in enumerate(data):
            text = item.get("text", "")
            
            if text in seen_texts:
                duplicates += 1
                self.warnings.append(f"Item {i}: Duplicate text found")
            else:
                seen_texts.add(text)
        
        if duplicates > 0:
            self.warnings.append(f"Total {duplicates} duplicate items found")
        
        # 检查关键词
        keyword_counts = {
            "user_input_parsing": 0,
            "job_matching": 0,
            "advice_generation": 0,
        }
        
        for item in data:
            text = item.get("text", "")
            if "解析用户的求职需求" in text or "提取关键信息" in text:
                keyword_counts["user_input_parsing"] += 1
            if "匹配合适的岗位" in text or "匹配结果" in text:
                keyword_counts["job_matching"] += 1
            if "职业规划" in text or "简历优化" in text:
                keyword_counts["advice_generation"] += 1
        
        logger.info(f"Task distribution: {keyword_counts}")
        
        # 检查任务分布是否均衡
        total = sum(keyword_counts.values())
        if total > 0:
            for task, count in keyword_counts.items():
                ratio = count / total
                if ratio < 0.15:  # 低于15%
                    self.warnings.append(f"Task '{task}' is underrepresented ({ratio:.1%})")
    
    def _check_statistics(self, data: List[Dict[str, Any]]):
        """检查统计信息"""
        logger.info("Computing statistics...")
        
        text_lengths = [len(item.get("text", "")) for item in data]
        
        stats = {
            "total_samples": len(data),
            "min_length": min(text_lengths) if text_lengths else 0,
            "max_length": max(text_lengths) if text_lengths else 0,
            "avg_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
        }
        
        logger.info(f"Dataset statistics: {stats}")
        
        # 检查样本数量
        if stats["total_samples"] < 1000:
            self.warnings.append(f"Dataset is small ({stats['total_samples']} samples), may need more data")
        
        # 检查长度分布
        if stats["max_length"] > 2048:
            self.warnings.append(f"Some samples are very long (max {stats['max_length']}), consider truncation")
    
    def generate_report(self) -> str:
        """生成校验报告"""
        report = "=" * 60 + "\n"
        report += "Data Validation Report\n"
        report += "=" * 60 + "\n\n"
        
        if self.errors:
            report += f"❌ ERRORS ({len(self.errors)}):\n"
            for i, error in enumerate(self.errors, 1):
                report += f"  {i}. {error}\n"
            report += "\n"
        else:
            report += "✓ No errors found\n\n"
        
        if self.warnings:
            report += f"⚠ WARNINGS ({len(self.warnings)}):\n"
            for i, warning in enumerate(self.warnings, 1):
                report += f"  {i}. {warning}\n"
            report += "\n"
        else:
            report += "✓ No warnings\n\n"
        
        report += "=" * 60 + "\n"
        
        return report


def main():
    """测试数据校验"""
    validator = DataValidator()
    
    # 校验训练集
    passed, errors, warnings = validator.validate_dataset("./data/train.jsonl")
    
    # 打印报告
    print(validator.generate_report())
    
    if not passed:
        print("❌ Validation failed!")
    else:
        print("✓ Validation passed!")


if __name__ == "__main__":
    main()

