"""
Script to update all imports from db_metadata_generator to data_dictionary_builder
Run this from the root of your project directory.
"""

import os
import re

def update_imports_in_file(filepath):
    """Update imports in a single file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Replace import statements
        # Pattern 1: from db_metadata_generator import ...
        content = re.sub(
            r'from db_metadata_generator',
            'from data_dictionary_builder',
            content
        )
        
        # Pattern 2: import db_metadata_generator
        content = re.sub(
            r'import db_metadata_generator',
            'import data_dictionary_builder',
            content
        )
        
        # Pattern 3: from ..metadata (relative imports stay the same)
        # These don't need changing
        
        # Only write if content changed
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Updated: {filepath}")
            return True
        else:
            print(f"  No changes: {filepath}")
            return False
    except Exception as e:
        print(f"✗ Error updating {filepath}: {e}")
        return False

def update_all_imports(root_dir):
    """Update imports in all Python files."""
    updated_count = 0
    total_count = 0
    
    # Directories to scan
    directories = [
        os.path.join(root_dir, 'src'),
        os.path.join(root_dir, 'examples'),
        os.path.join(root_dir, 'tests'),
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            print(f"Directory not found: {directory}")
            continue
        
        print(f"\nScanning: {directory}")
        
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if filename.endswith('.py'):
                    filepath = os.path.join(root, filename)
                    total_count += 1
                    if update_imports_in_file(filepath):
                        updated_count += 1
    
    print(f"\n{'='*60}")
    print(f"Summary: Updated {updated_count} out of {total_count} files")
    print(f"{'='*60}")

if __name__ == '__main__':
    # Run from project root
    project_root = os.getcwd()
    print(f"Project root: {project_root}")
    print(f"Starting import updates...\n")
    
    update_all_imports(project_root)
    
    print("\n✓ Done! Now run:")
    print("  pip uninstall data-dictionary-builder -y")
    print("  pip install -e .")
    print("  python -c \"from data_dictionary_builder import MetadataExtractor; print('Success!')\"")
