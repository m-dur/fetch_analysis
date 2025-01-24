import json
from pathlib import Path
from typing import Dict, List, Any, Union
from collections import defaultdict
import os

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / 'raw_data'

class DataWarehouseModeler:
    def __init__(self):
        self.type_mapping = {
            str: "VARCHAR(255)",
            int: "INTEGER",
            float: "DECIMAL(10,2)",
            bool: "BOOLEAN",
            type(None): "NULL",
            dict: "NESTED",
            list: "ARRAY"
        }
        self.current_file_stem = None
        # Add exception tables structure
        self.exception_tables = {
            "missing_brands": {
                "columns": {
                    "missing_brands_id": {"SERIAL PRIMARY KEY"},
                    "brandcode": {"VARCHAR(255) UNIQUE"},
                    "occurrence_count": {"INTEGER"},
                    "first_seen": {"TIMESTAMP"},
                    "last_seen": {"TIMESTAMP"}
                },
                "relationships": {
                    "tracks missing from relationship with rewardsreceiptitemlist"
                }
            },
            "missing_users": {
                "columns": {
                    "missing_users_id": {"SERIAL PRIMARY KEY"},
                    "user_id": {"VARCHAR(24) UNIQUE"},
                    "occurrence_count": {"INTEGER"},
                    "first_seen": {"TIMESTAMP"},
                    "last_seen": {"TIMESTAMP"}
                },
                "relationships": {
                    "tracks missing from relationship with receipts"
                }
            }
        }
        self.exception_tables_added = False  # Add this flag
        
    def clean_column_name(self, name: str) -> str:
        """Clean column names to be SQL compliant"""
        return name.replace('$', '').replace('.', '_').lower()
    
    def infer_data_type(self, value: Any, column_name: str) -> str:
        """Infer SQL data type from Python value and column name"""
        if value is None:
            return "NULL"
            
        if isinstance(value, dict):
            if '$date' in value:  # MongoDB date format
                return "TIMESTAMP"
            if '$oid' in value:   # MongoDB ObjectId
                return "VARCHAR(24)"
                
        value_type = type(value)
        if value_type == str:
            if 'date' in column_name.lower():
                return "TIMESTAMP"
            if 'price' in column_name.lower() or 'spent' in column_name.lower():
                return "DECIMAL(10,2)"
            return "TEXT" if len(str(value)) > 255 else "VARCHAR(255)"
            
        return self.type_mapping.get(value_type, "VARCHAR(255)")

    def analyze_structure(self, data: Union[Dict, List]) -> Dict:
        """Recursively analyze JSON structure and suggest table designs"""
        structure = {
            "tables": defaultdict(lambda: {"columns": defaultdict(set), "relationships": set()})
        }
        
        if isinstance(data, list):
            for item in data:
                self._analyze_recursive(item, "", structure)
        else:
            self._analyze_recursive(data, "", structure)

        # Add exception tables only once
        if not self.exception_tables_added:
            for table_name, table_info in self.exception_tables.items():
                structure["tables"][table_name] = table_info
            self.exception_tables_added = True

        return structure

    def _analyze_recursive(self, item: Any, current_path: str, structure: Dict) -> None:
        if isinstance(item, dict):
            table_name = current_path.split('.')[0] if current_path else self.current_file_stem
            
            for key, value in item.items():
                clean_key = self.clean_column_name(key)
                
                # Handle relationships based on known foreign keys
                if clean_key == 'userid' and table_name == 'receipts':
                    data_type = "VARCHAR(24)"
                    structure["tables"][table_name]["columns"][clean_key].add(data_type)
                    structure["tables"][table_name]["relationships"].add(
                        f"foreign key relationship: {table_name}.{clean_key} -> users._id")
                    # Add relationship with missing_users table
                    structure["tables"][table_name]["relationships"].add(
                        f"tracked by missing_users when not in users table")
                    continue
                
                # Handle brandcode relationships
                if clean_key == 'brandcode' and table_name == 'rewardsreceiptitemlist':
                    data_type = "VARCHAR(255)"
                    structure["tables"][table_name]["columns"][clean_key].add(data_type)
                    structure["tables"][table_name]["relationships"].add(
                        f"foreign key relationship: {table_name}.{clean_key} -> brands.brandcode")
                    # Add relationship with missing_brands table
                    structure["tables"][table_name]["relationships"].add(
                        f"tracked by missing_brands when not in brands table")
                    continue
                
                # Handle category as separate table
                if clean_key == 'category' and table_name == 'brands':
                    structure["tables"]["categories"] = {
                        "columns": {
                            "category": {"VARCHAR(255)"},
                            "categorycode": {"VARCHAR(255)"}
                        },
                        "relationships": {
                            "one_to_many relationship with brands (category_id)"
                        }
                    }
                    structure["tables"][table_name]["relationships"].add(
                        f"foreign key relationship: {table_name}.category_id -> categories.category_id")
                    continue
                
                # Skip categorycode as it's now part of categories table
                if clean_key == 'categorycode' and table_name == 'brands':
                    continue
                
                # Remove cpg handling
                if clean_key == 'cpg':
                    continue
                
                if isinstance(value, dict):
                    if '$date' in value or '$oid' in value:
                        data_type = self.infer_data_type(value, clean_key)
                        structure["tables"][table_name]["columns"][clean_key].add(data_type)
                    else:
                        # Skip creating cpg relationships
                        if clean_key != 'cpg':
                            self._analyze_recursive(value, clean_key, structure)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    if clean_key == 'rewardsreceiptitemlist':
                        structure["tables"][table_name]["relationships"].add(
                            f"one_to_many relationship with rewardsreceiptitemlist (receipt_id)")
                    self._analyze_recursive(value[0], clean_key, structure)
                else:
                    data_type = self.infer_data_type(value, clean_key)
                    structure["tables"][table_name]["columns"][clean_key].add(data_type)

    def generate_ddl(self, structure: Dict) -> str:
        """Generate DDL statements from analyzed structure"""
        ddl = []
        
        # Create tables, including exception tables
        for table_name, table_info in structure["tables"].items():
            columns = []
            
            # Handle primary key
            if table_name in self.exception_tables:
                # Use predefined columns for exception tables
                columns = [f"{col} {' '.join(types)}" 
                          for col, types in table_info["columns"].items()]
            else:
                # Regular table handling
                columns.append(f"{table_name}_id SERIAL PRIMARY KEY")
                for col_name, data_types in table_info["columns"].items():
                    data_type = max(data_types, key=lambda x: 0 if x == "NULL" else 1)
                    nullable = "NULL" in data_types
                    columns.append(f"{col_name} {data_type}" + 
                                 (" NULL" if nullable else " NOT NULL"))
            
            create_table = f"CREATE TABLE {table_name} (\n"
            create_table += "    " + ",\n    ".join(columns)
            create_table += "\n);"
            ddl.append(create_table)
            
            # Add indexes for exception tables
            if table_name == 'missing_brands':
                ddl.append(f"CREATE INDEX idx_{table_name}_brandcode ON {table_name}(brandcode);")
            elif table_name == 'missing_users':
                ddl.append(f"CREATE INDEX idx_{table_name}_user_id ON {table_name}(user_id);")
        
        return "\n\n".join(ddl)

def read_json_file(file_path: Path) -> List[Dict]:
    data = []
    try:
        # First try reading as regular JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                content = json.load(file)
                if isinstance(content, list):
                    data.extend(content)
                else:
                    data.append(content)
            except json.JSONDecodeError:
                # If that fails, try reading as JSONL
                file.seek(0)
                for line in file:
                    if line.strip():
                        try:
                            item = json.loads(line.strip())
                            data.append(item)
                        except json.JSONDecodeError:
                            continue
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        print(f"File path attempted: {file_path}")
        print(f"Current working directory: {os.getcwd()}")
        return []
    
    return data

def analyze_json_file(file_path: str) -> None:
    try:
        # Handle path resolution
        file_path = DATA_DIR / file_path
        print(f"Attempting to read: {file_path}")

        # Get the file name without extension to use as default table name
        file_stem = file_path.stem

        data = read_json_file(file_path)
        if not data:
            print("No valid JSON data found in file")
            return

        modeler = DataWarehouseModeler()
        modeler.current_file_stem = file_stem  # Set the current file stem
        structure = modeler.analyze_structure(data)
        
        # Print analysis results
        print(f"\n=== Table Structure for {file_stem} ===\n")
        
        for table_name, table_info in structure["tables"].items():
            print(f"\nTable: {table_name}")
            print("Columns:")
            for col, types in table_info["columns"].items():
                print(f"  - {col}: {' | '.join(types)}")
            
            if table_info["relationships"]:
                print("Relationships:")
                for rel in table_info["relationships"]:
                    print(f"  - {rel}")
                    
        return structure

    except Exception as e:
        print(f"Error analyzing {file_path}: {str(e)}")
        return None

if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    if len(sys.argv) < 2:
        print("Usage: python dw_modeler.py <json_file1> [json_file2] [json_file3] ...")
        print("\nAvailable JSON files:")
        for json_file in DATA_DIR.glob('*.json'):
            print(f"- {json_file.name}")
        sys.exit(1)
    
    # Process all input files
    all_structures = {}
    
    for file_path in sys.argv[1:]:
        try:
            # Handle path resolution
            json_path = DATA_DIR / file_path
            print(f"\nAnalyzing: {json_path}")
            
            data = read_json_file(json_path)
            if not data:
                print(f"No valid JSON data found in {file_path}")
                continue
                
            modeler = DataWarehouseModeler()
            file_stem = Path(file_path).stem  # Convert string to Path object to get stem
            modeler.current_file_stem = file_stem
            structure = modeler.analyze_structure(data)
            
            all_structures[file_stem] = structure
            
        except Exception as e:
            print(f"Error analyzing {file_path}: {str(e)}")
    
    # Analyze potential relationships between tables
    relationships = []
    for table1, struct1 in all_structures.items():
        for table2, struct2 in all_structures.items():
            if table1 != table2:
                # Look for matching column names that could indicate relationships
                cols1 = set(next(iter(struct1["tables"].values()))["columns"].keys())
                cols2 = set(next(iter(struct2["tables"].values()))["columns"].keys())
                
                # Find potential foreign keys
                for col in cols1:
                    if col.endswith('_id') or col.endswith('Id'):
                        base_name = col.replace('_id', '').replace('Id', '').lower()
                        if base_name == table2.lower():
                            relationships.append(f"Potential foreign key: {table1}.{col} -> {table2}")
    
    # Print combined analysis
    print("\n=== Combined Data Warehouse Structure ===\n")
    
    for file_stem, structure in all_structures.items():
        print(f"\nFile: {file_stem}")
        for table_name, table_info in structure["tables"].items():
            print(f"\nTable: {table_name}")
            print("Columns:")
            for col, types in table_info["columns"].items():
                print(f"  - {col}: {' | '.join(types)}")
            
            if table_info["relationships"]:
                print("Internal Relationships:")
                for rel in table_info["relationships"]:
                    print(f"  - {rel}")
    
    if relationships:
        print("\nPotential Cross-File Relationships:")
        for rel in relationships:
            print(f"  - {rel}")