# -*- encoding: UTF-8 -*-

import akshare as ak
from industry_daliyK_analysis import IndustryAnalyzer
import push
import settings
from datetime import datetime, timedelta
import logging

class MACrossoverMonitor:
    """
    均线交叉监控器
    监控所有行业板块的均线交叉信号，并推送汇总消息
    """
    
    def __init__(self):
        """初始化监控器"""
        self.analyzer = IndustryAnalyzer()
        
    def get_all_industries(self):
        """
        获取所有行业板块列表
        
        Returns:
            list: 行业板块名称列表
        """
        try:
            # 获取行业板块数据
            # df = ak.stock_board_industry_name_em()
            # 获取股票代码数据
            stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
            return stock_zh_a_spot_em_df['代码'].tolist()
        except Exception as e:
            logging.error(f"获取行业板块列表失败: {e}")
            return []
    
    def analyze_industry_crossovers(self, industry_name, days_back=60):
        """
        分析单个行业的均线交叉信号
        
        Args:
            industry_name: 行业名称
            days_back: 回看天数
            
        Returns:
            dict: 分析结果
        """
        try:
            result = self.analyzer.analyze_comprehensive_technical(
                symbol=industry_name, 
                days_back=days_back, 
                data_source="industry"
            )
            
            if "error" in result:
                return {"error": result["error"]}
                
            # 提取均线交叉信号
            crossover_signals = result.get("均线交叉信号", [])
            recent_signals = [signal for signal in crossover_signals if signal["天数前"] <= 5]
            
            return {
                "industry": industry_name,
                "latest_date": result["最新日期"],
                "latest_price": result["最新收盘价"],
                "crossover_signals": recent_signals,
                "ma_arrangement": result["均线分析"]["排列状态"],
                "signal_strength": result["均线分析"]["信号强度"],
                "overall_rating": result["综合评级"]
            }
            
        except Exception as e:
            logging.error(f"分析行业 {industry_name} 失败: {e}")
            return {"error": str(e)}
    
    def scan_all_industries(self):
        """
        扫描所有行业的均线交叉信号
        
        Returns:
            dict: 扫描结果汇总
        """
        industries = self.get_all_industries()
        if not industries:
            return {"error": "无法获取行业列表"}
        
        results = {
            "golden_cross": [],      # 金叉信号
            "death_cross": [],       # 死叉信号
            "strong_bullish": [],    # 强多头排列
            "strong_bearish": [],    # 强空头排列
            "oversold": [],          # 超跌板块
            "errors": []             # 错误记录
        }
        
        total_industries = len(industries)
        processed = 0
        
        for industry in industries:
            processed += 1
            print(f"处理进度: {processed}/{total_industries} - {industry}")
            
            analysis = self.analyze_industry_crossovers(industry)
            
            if "error" in analysis:
                results["errors"].append({
                    "industry": industry,
                    "error": analysis["error"]
                })
                continue
            
            # 分类处理信号
            crossover_signals = analysis["crossover_signals"]
            
            for signal in crossover_signals:
                signal_info = {
                    "industry": industry,
                    "signal_type": signal["类型"],
                    "fast_ma": signal["快线"],
                    "slow_ma": signal["慢线"],
                    "days_ago": signal["天数前"],
                    "strength": signal["信号强度"],
                    "latest_price": analysis["latest_price"],
                    "date": analysis["latest_date"]
                }
                
                if signal["类型"] == "金叉":
                    results["golden_cross"].append(signal_info)
                elif signal["类型"] == "死叉":
                    results["death_cross"].append(signal_info)
            
            # 检查均线排列状态
            if analysis["ma_arrangement"] in ["完美多头排列", "多头排列"]:
                if analysis["signal_strength"] >= 4:
                    results["strong_bullish"].append({
                        "industry": industry,
                        "arrangement": analysis["ma_arrangement"],
                        "strength": analysis["signal_strength"],
                        "latest_price": analysis["latest_price"]
                    })
            elif analysis["ma_arrangement"] in ["完美空头排列", "空头排列"]:
                if analysis["signal_strength"] <= -4:
                    results["strong_bearish"].append({
                        "industry": industry,
                        "arrangement": analysis["ma_arrangement"],
                        "strength": analysis["signal_strength"],
                        "latest_price": analysis["latest_price"]
                    })
            
            # 检查超跌情况
            if analysis["overall_rating"] in ["强烈超跌", "可能超跌"]:
                results["oversold"].append({
                    "industry": industry,
                    "rating": analysis["overall_rating"],
                    "latest_price": analysis["latest_price"]
                })
        
        return results
    
    def format_message(self, results):
        """
        格式化推送消息
        
        Args:
            results: 扫描结果
            
        Returns:
            str: 格式化的消息
        """
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message_parts = [f"📊 行业均线交叉监控报告\n🕐 {current_time}\n"]
        
        # 金叉信号
        if results["golden_cross"]:
            message_parts.append("🟢 近期金叉信号:")
            golden_by_strength = {}
            for signal in results["golden_cross"]:
                strength = signal["strength"]
                if strength not in golden_by_strength:
                    golden_by_strength[strength] = []
                golden_by_strength[strength].append(signal)
            
            for strength in sorted(golden_by_strength.keys(), reverse=True):
                signals = golden_by_strength[strength]
                message_parts.append(f"  {strength}信号 ({len(signals)}个):")
                for signal in signals[:5]:  # 最多显示5个
                    message_parts.append(f"    • {signal['industry']} {signal['fast_ma']}×{signal['slow_ma']} ({signal['days_ago']}天前)")
                if len(signals) > 5:
                    message_parts.append(f"    ... 还有{len(signals)-5}个")
            message_parts.append("")
        
        # 死叉信号
        if results["death_cross"]:
            message_parts.append("🔴 近期死叉信号:")
            death_by_strength = {}
            for signal in results["death_cross"]:
                strength = signal["strength"]
                if strength not in death_by_strength:
                    death_by_strength[strength] = []
                death_by_strength[strength].append(signal)
            
            for strength in sorted(death_by_strength.keys(), reverse=True):
                signals = death_by_strength[strength]
                message_parts.append(f"  {strength}信号 ({len(signals)}个):")
                for signal in signals[:5]:  # 最多显示5个
                    message_parts.append(f"    • {signal['industry']} {signal['fast_ma']}×{signal['slow_ma']} ({signal['days_ago']}天前)")
                if len(signals) > 5:
                    message_parts.append(f"    ... 还有{len(signals)-5}个")
            message_parts.append("")
        
        # 强多头排列
        if results["strong_bullish"]:
            message_parts.append(f"📈 强多头排列 ({len(results['strong_bullish'])}个):")
            for item in results["strong_bullish"][:8]:  # 最多显示8个
                message_parts.append(f"  • {item['industry']} ({item['arrangement']}, 强度{item['strength']})")
            if len(results["strong_bullish"]) > 8:
                message_parts.append(f"  ... 还有{len(results['strong_bullish'])-8}个")
            message_parts.append("")
        
        # 强空头排列
        if results["strong_bearish"]:
            message_parts.append(f"📉 强空头排列 ({len(results['strong_bearish'])}个):")
            for item in results["strong_bearish"][:8]:  # 最多显示8个
                message_parts.append(f"  • {item['industry']} ({item['arrangement']}, 强度{item['strength']})")
            if len(results["strong_bearish"]) > 8:
                message_parts.append(f"  ... 还有{len(results['strong_bearish'])-8}个")
            message_parts.append("")
        
        # 超跌板块
        if results["oversold"]:
            message_parts.append(f"⚡ 超跌板块 ({len(results['oversold'])}个):")
            for item in results["oversold"][:8]:  # 最多显示8个
                message_parts.append(f"  • {item['industry']} ({item['rating']})")
            if len(results["oversold"]) > 8:
                message_parts.append(f"  ... 还有{len(results['oversold'])-8}个")
            message_parts.append("")
        
        # 统计汇总
        total_golden = len(results["golden_cross"])
        total_death = len(results["death_cross"])
        total_bullish = len(results["strong_bullish"])
        total_bearish = len(results["strong_bearish"])
        total_oversold = len(results["oversold"])
        total_errors = len(results["errors"])
        
        message_parts.append("📋 统计汇总:")
        message_parts.append(f"  金叉信号: {total_golden}个")
        message_parts.append(f"  死叉信号: {total_death}个")
        message_parts.append(f"  强多头排列: {total_bullish}个")
        message_parts.append(f"  强空头排列: {total_bearish}个")
        message_parts.append(f"  超跌板块: {total_oversold}个")
        if total_errors > 0:
            message_parts.append(f"  分析失败: {total_errors}个")
        
        # 投资建议
        message_parts.append("\n💡 投资建议:")
        if total_golden > total_death:
            message_parts.append("  市场整体偏暖，关注金叉强信号的行业")
        elif total_death > total_golden:
            message_parts.append("  市场整体偏冷，注意风险控制")
        else:
            message_parts.append("  市场分化明显，精选个股为主")
        
        if total_oversold > 0:
            message_parts.append("  部分行业出现超跌，可关注反弹机会")
        
        message_parts.append("\n⚠️ 以上分析仅供参考，投资需谨慎！")
        
        return "\n".join(message_parts)
    
    def run_monitor_and_push(self):
        """
        运行监控并推送消息
        """
        try:
            logging.info("开始扫描所有行业均线交叉信号...")
            
            # 扫描所有行业
            results = self.scan_all_industries()
            
            if "error" in results:
                error_msg = f"监控失败: {results['error']}"
                logging.error(error_msg)
                push.push(error_msg)
                return
            
            # 格式化消息
            message = self.format_message(results)
            
            # 推送消息
            logging.info("推送均线交叉监控报告...")
            push.push(message)
            
            # 同时输出到控制台
            print("="*60)
            print(message)
            print("="*60)
            
        except Exception as e:
            error_msg = f"均线交叉监控过程中发生错误: {str(e)}"
            logging.error(error_msg)
            push.push(error_msg)


def main():
    """主函数"""
    # 初始化设置
    settings.init()
    
    # 创建监控器并运行
    monitor = MACrossoverMonitor()
    monitor.run_monitor_and_push()


if __name__ == "__main__":
    main()