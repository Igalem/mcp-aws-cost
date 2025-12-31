"""Report formatting utilities for cost analysis results."""

from typing import Dict, Any, List
import json


def format_analysis_report(results: Dict[str, Any]) -> str:
    """
    Format analysis results as a readable text report.
    
    Args:
        results: Dictionary containing analysis results
        
    Returns:
        Formatted text report
    """
    lines = []
    lines.append("=" * 80)
    lines.append("COST ANALYSIS REPORT")
    lines.append("=" * 80)
    lines.append("")
    
    # Summary section
    if 'summary' in results:
        lines.append("SUMMARY")
        lines.append("-" * 80)
        for key, value in results['summary'].items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Daily metrics
    if 'daily_metrics' in results:
        lines.append("DAILY METRICS")
        lines.append("-" * 80)
        for metric in results['daily_metrics']:
            lines.append(f"  {metric}")
        lines.append("")
    
    # Period comparison
    if 'period_comparison' in results:
        lines.append("PERIOD COMPARISON")
        lines.append("-" * 80)
        for key, value in results['period_comparison'].items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Query patterns
    if 'query_patterns' in results:
        lines.append("QUERY PATTERNS")
        lines.append("-" * 80)
        for pattern in results['query_patterns']:
            lines.append(f"  {pattern}")
        lines.append("")
    
    # Top expensive queries
    if 'top_queries' in results:
        lines.append("TOP EXPENSIVE QUERIES")
        lines.append("-" * 80)
        for query in results['top_queries']:
            lines.append(f"  {query}")
        lines.append("")
    
    lines.append("=" * 80)
    return "\n".join(lines)


def format_comparison_report(results: Dict[str, Any]) -> str:
    """
    Format comparison results as a readable text report.
    
    Args:
        results: Dictionary containing comparison results
        
    Returns:
        Formatted text report
    """
    lines = []
    lines.append("=" * 80)
    lines.append("QUERY COMPARISON REPORT")
    lines.append("=" * 80)
    lines.append("")
    
    # Query details
    if 'query_details' in results:
        lines.append("QUERY DETAILS")
        lines.append("-" * 80)
        for key, value in results['query_details'].items():
            lines.append(f"  {key}: {value}")
        lines.append("")
    
    # Statistics
    if 'statistics' in results:
        lines.append("STATISTICS")
        lines.append("-" * 80)
        for stat in results['statistics']:
            lines.append(f"  {stat}")
        lines.append("")
    
    # Patterns
    if 'patterns' in results:
        lines.append("PATTERNS")
        lines.append("-" * 80)
        for pattern in results['patterns']:
            lines.append(f"  {pattern}")
        lines.append("")
    
    lines.append("=" * 80)
    return "\n".join(lines)


def format_json_report(results: Dict[str, Any]) -> str:
    """
    Format results as JSON.
    
    Args:
        results: Dictionary containing results
        
    Returns:
        JSON formatted string
    """
    return json.dumps(results, indent=2, default=str)



