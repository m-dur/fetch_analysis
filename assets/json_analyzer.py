import json
import pandas as pd
from typing import Dict, Set, List, Any, Tuple, Union

def analyze_json_file(file_path: str) -> Dict[str, Any]:
    """
    Analyze a JSON file and return statistics about its structure.
    
    Args:
        file_path: Path to the JSON file to analyze
        
    Returns:
        Dictionary containing analysis results or error information
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            file.seek(0)  
            try:
                data = [json.loads(line) for line in file if line.strip()]
            except json.JSONDecodeError as e:
                return {"status": "error", "message": f"Failed to parse as JSON or JSON Lines: {str(e)}"}
            except Exception as e:
                return {"status": "error", "message": f"Unexpected error: {str(e)}"}

    if data:
        def analyze_json(
            data: Union[Dict, List, Any], 
            level: int = 0, 
            key_stats: Dict[str, Union[Set[str], Dict[str, List[int]], List[Tuple[Any, int]]]] = None
        ) -> Dict[str, Union[Set[str], Dict[str, List[int]], List[Tuple[Any, int]]]]:
            """
            Recursively analyze JSON data structure to gather statistics about keys and their depths.
            
            Args:
                data: The JSON data to analyze
                level: Current depth level in the JSON structure
                key_stats: Dictionary to store analysis results
                
            Returns:
                Dictionary containing analysis results
            """
            if key_stats is None:
                key_stats = {"total_keys": set(), "key_depths": {}, "abnormalities": []}

            if isinstance(data, dict):
                for key, value in data.items():
                    key_stats["total_keys"].add(key)
                    if key not in key_stats["key_depths"]:
                        key_stats["key_depths"][key] = []
                    key_stats["key_depths"][key].append(level)
                    analyze_json(value, level + 1, key_stats)
            elif isinstance(data, list):
                for item in data:
                    analyze_json(item, level, key_stats)
            else:
                if not isinstance(data, (str, int, float, bool, type(None))):
                    key_stats["abnormalities"].append((data, level))

            return key_stats

        stats = analyze_json(data)

        # Prepare stats summary
        stats_summary = {
            "status": "success",
            "total_unique_keys": len(stats["total_keys"]),
            "keys_and_their_depths": {k: sorted(list(set(v))) for k, v in stats["key_depths"].items()},
            "abnormal_data_points": stats["abnormalities"]
        }

        # Prepare a DataFrame for visualization of key depth analysis
        depth_analysis = pd.DataFrame([
            {"key": key, "depths": sorted(list(depths))}
            for key, depths in stats_summary["keys_and_their_depths"].items()
        ])
        
        print("\nJSON Key Depth Analysis:")
        print(depth_analysis) 

        return stats_summary
    else:
        return {"status": "error", "message": error_message}

if __name__ == "__main__":
    file_path = '../raw_data/receipts.json'
    result = analyze_json_file(file_path)
    print(result)